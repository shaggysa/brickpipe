import asyncio
import json
import os
import struct
import sys
from enum import Enum
from subprocess import CalledProcessError

import reactivex.operators as op
from bleak import BLEDevice, AdvertisementData, BleakScanner
from packaging.version import Version
from pybricksdev.ble import find_device as find_ble
from pybricksdev.ble.pybricks import (
    PYBRICKS_COMMAND_EVENT_UUID,
    Command,
    StatusFlag,
)
from pybricksdev.ble.pybricks import PYBRICKS_SERVICE_UUID
from pybricksdev.cli import _get_script_path
from pybricksdev.connections.pybricks import PybricksHubBLE, HubDisconnectError, PybricksHub
from pybricksdev.connections.pybricks import PybricksHubUSB
from pybricksdev.tools import chunk
from pybricksdev.usb import (
    EV3_USB_PID,
    LEGO_USB_VID,
    MINDSTORMS_INVENTOR_USB_PID,
    NXT_USB_PID,
    SPIKE_ESSENTIAL_USB_PID,
    SPIKE_PRIME_USB_PID,
)
from usb.core import find as find_usb

incoming_messages = asyncio.Queue()


class IncomingEventType(str, Enum):
    # `timeout (int | float, seconds)`
    start_ble_scanning = 'start_ble_scanning'

    # `conn_type: "ble" | "usb"`
    # `ble_address` (optional)
    # `ble_hub_name` (optional)
    connect_to_hub = 'connect_to_hub'

    disconnect_from_hub = 'disconnect_from_hub'

    # `program_path`
    recompile_download = 'recompile_download'

    # `program_path`
    recompile_run = 'recompile_run'

    run_stored = 'run_stored'

    # `string`
    send_string = 'send_string'

    cancel_running_program = 'cancel_running_program'

    exit = 'exit'


class OutgoingEventType(str, Enum):
    # `device_name`
    # `address`
    # `rssi: int`
    ble_device_found = 'ble_device_found'

    hub_connected = 'hub_connected'

    connection_timeout = 'connection_timeout'

    # `percentage (0.00 - 100.00)`
    download_progress_update = 'download_progress_update'

    program_started = 'program_started'

    program_complete = 'program_complete'

    # `line`
    hub_printed_line = 'hub_printed_line'

    # `traceback`
    compile_error = 'compile_error'

    # `explanation`
    precondition_violated = 'precondition_violated'

    # `explanation`
    hub_firmware_outdated = 'hub_firmware_outdated'

    hub_disconnected = 'hub_disconnected'


async def send_event(event_type: OutgoingEventType, payload: dict | None = None):
    if payload is None:
        payload = {"event_type": event_type.value}
    else:
        payload.update({"event_type": event_type.value})
    print(json.dumps(payload), flush=True)


async def watch_incoming_events():
    while True:
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            await incoming_messages.put({"event_type": IncomingEventType.exit.value})
            break
        try:
            data = json.loads(line.strip())
            await incoming_messages.put(data)
        except json.decoder.JSONDecodeError:
            await send_event(OutgoingEventType.precondition_violated, {"explanation": "received invalid json"})


def is_pybricks_usb(dev):
    return (
            (dev.idVendor == LEGO_USB_VID)
            and (
                    dev.idProduct
                    in [
                        NXT_USB_PID,
                        EV3_USB_PID,
                        SPIKE_PRIME_USB_PID,
                        SPIKE_ESSENTIAL_USB_PID,
                        MINDSTORMS_INVENTOR_USB_PID,
                    ]
            )
            and dev.product.endswith("Pybricks")
    )


async def observe_running_status(hub: PybricksHub):
    user_program_running: asyncio.Queue[bool] = asyncio.Queue()

    with hub.status_observable.pipe(
            op.map(lambda s: bool(s & StatusFlag.USER_PROGRAM_RUNNING)),
            op.distinct_until_changed(),
    ).subscribe(lambda s: user_program_running.put_nowait(s)):
        is_running = await hub.race_disconnect(user_program_running.get())
        if is_running:
            await send_event(OutgoingEventType.program_started)
        # don't do anything if a program isn't running, the hub was just connected
        try:
            while True:
                is_running = await hub.race_disconnect(user_program_running.get())
                if is_running:
                    await send_event(OutgoingEventType.program_started)
                else:
                    await send_event(OutgoingEventType.program_complete)
        except asyncio.CancelledError:
            pass


async def observe_stdout(hub: PybricksHub):
    try:
        while True:
            line = await hub.read_line()
            if line:
                await send_event(OutgoingEventType.hub_printed_line, {"line": line})
    except asyncio.CancelledError:
        pass


async def observe_hub(hub: PybricksHub):
    hub._enable_line_handler = True
    hub.print_output = False

    stdout_task = asyncio.create_task(observe_stdout(hub))
    running_task = asyncio.create_task(observe_running_status(hub))

    try:
        await hub.race_disconnect(asyncio.Future())
    finally:
        stdout_task.cancel()
        running_task.cancel()


# hub.download calls a download_user_program method internally,
# which prints progress to the terminal. This function
# overrides it to send progress events instead
async def download_user_program_override(self: PybricksHub, program: bytes):
    # the hub tells us the max size of program that is allowed, so we can fail early
    if len(program) > self._max_user_program_size:
        raise ValueError(
            f"program is too big ({len(program)} bytes). Hub has limit of {self._max_user_program_size} bytes."
        )

    # clear user program meta so hub doesn't try to run invalid program
    await self.write_gatt_char(
        PYBRICKS_COMMAND_EVENT_UUID,
        struct.pack("<BI", Command.WRITE_USER_PROGRAM_META, 0),
        response=True,
    )

    # payload is max size minus header size
    payload_size = self._max_write_size - 5

    bytes_sent = 0

    # write program data while sending progress events
    for i, c in enumerate(chunk(program, payload_size)):
        await self.write_gatt_char(
            PYBRICKS_COMMAND_EVENT_UUID,
            struct.pack(
                f"<BI{len(c)}s",
                Command.COMMAND_WRITE_USER_RAM,
                i * payload_size,
                c,
            ),
            response=True,
        )
        bytes_sent += (len(c))
        percentage = round((bytes_sent / len(program)) * 100, 2)

        await send_event(OutgoingEventType.download_progress_update, {"percentage": percentage})

    # set the metadata to notify that writing was successful
    await self.write_gatt_char(
        PYBRICKS_COMMAND_EVENT_UUID,
        struct.pack("<BI", Command.WRITE_USER_PROGRAM_META, len(program)),
        response=True,
    )


async def ble_scanner_callback(device: BLEDevice, adv: AdvertisementData):
    if PYBRICKS_SERVICE_UUID in adv.service_uuids and adv.local_name:
        await send_event(OutgoingEventType.ble_device_found,
                         {"address": device.address, "device_name": device.name, "rssi": adv.rssi})


async def main_loop():
    ble_scanner = BleakScanner(ble_scanner_callback)
    asyncio.create_task(watch_incoming_events())

    hub = None
    hub_monitor_tasks = None
    ble_scan_stop_event = asyncio.Event()

    while True:
        try:
            if hub:
                command = await hub.race_disconnect(incoming_messages.get())
            else:
                command = await incoming_messages.get()

            if 'event_type' not in command:
                await send_event(OutgoingEventType.precondition_violated,
                                 {'explanation': 'all events must have an "event_type" tag'})
                continue

            ble_scan_stop_event.set()

            match command.get('event_type'):
                case IncomingEventType.start_ble_scanning:
                    if 'timeout' not in command:
                        await send_event(OutgoingEventType.precondition_violated,
                                         {
                                             'explanation': 'a "start_ble_scanning" command must have a "timeout argument"'})

                    timeout = command.get('timeout')

                    if type(timeout) is not float and type(timeout) is not int:
                        await send_event(OutgoingEventType.precondition_violated,
                                         {
                                             'explanation': 'the "timeout" argument must be an integer or floating-point number'})

                    ble_scan_stop_event.clear()

                    async def scan_ble(timeout: float | int):
                        async with BleakScanner(ble_scanner_callback) as scanner:
                            try:
                                await asyncio.wait_for(ble_scan_stop_event.wait(), timeout)
                            except TimeoutError:
                                pass

                    asyncio.create_task(scan_ble(timeout))

                case IncomingEventType.connect_to_hub:
                    if hub:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub is already connected'})
                    else:
                        if 'ble_hub_name' in command:
                            name = command.get('ble_hub_name')
                        else:
                            name = None

                        if 'conn_type' not in command:
                            await send_event(OutgoingEventType.precondition_violated,
                                             {
                                                 'explanation': 'a "connect_to_hub" command must have an "conn_type" argument"'})
                            continue

                        conntype = command.get('conn_type')

                        if conntype == 'ble':
                            try:
                                if 'ble_address' in command:
                                    device_or_address = await BleakScanner.find_device_by_address(
                                        command.get('ble_address'))
                                    if device_or_address is None:
                                        raise TimeoutError
                                else:
                                    device_or_address = await find_ble(name)

                                hub = PybricksHubBLE(device_or_address)
                            except TimeoutError:
                                await send_event(OutgoingEventType.connection_timeout)
                        elif conntype == 'usb':
                            device_or_address = find_usb(custom_match=is_pybricks_usb)
                            if device_or_address is None:
                                await send_event(OutgoingEventType.connection_timeout)
                                continue
                            hub = PybricksHubUSB(device_or_address)
                        else:
                            await send_event(OutgoingEventType.precondition_violated,
                                             payload={
                                                 'explanation': 'usb and ble are the only valid connection types'})

                        if hub:
                            await hub.connect()
                            if hub.fw_version < Version("3.2.0-beta.4"):
                                await hub.disconnect()
                                await send_event(OutgoingEventType.hub_firmware_outdated,
                                                 {"explanation": "this tool requires hub firmware version >= 3.2.0"})
                                continue

                            hub_monitor_tasks = asyncio.create_task(observe_hub(hub))
                            await send_event(OutgoingEventType.hub_connected)

                case IncomingEventType.disconnect_from_hub:
                    if hub:
                        await hub.disconnect()
                        hub = None
                        if hub_monitor_tasks:
                            # the tasks should already be canceled when the hub is disconnected,
                            # but double-cancelling has no negative side effects
                            hub_monitor_tasks.cancel()
                            hub_monitor_tasks = None

                    await send_event(OutgoingEventType.hub_disconnected)

                case IncomingEventType.recompile_download:
                    if hub:
                        # the hub doesn't like data being sent while a program is running
                        # calling stop does nothing if a program isn't running
                        await hub.stop_user_program()

                        if 'program_path' not in command:
                            await send_event(OutgoingEventType.precondition_violated,
                                             payload={
                                                 'explanation': '"recompile_download" events must have a "program_path" argument'})
                            continue

                        program_path = command.get('program_path')

                        try:
                            with _get_script_path(open(program_path)) as script_path:
                                await hub.download(script_path)
                        except FileNotFoundError:
                            await send_event(OutgoingEventType.precondition_violated,
                                             payload={'explanation': 'received file path is not valid'})
                    else:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub must be connected to download a program'})

                case IncomingEventType.recompile_run:
                    if hub:
                        # the hub doesn't like data being sent while a program is running
                        # calling stop does nothing if a program isn't running
                        await hub.stop_user_program()

                        if 'program_path' not in command:
                            await send_event(OutgoingEventType.precondition_violated,
                                             payload={
                                                 'explanation': '"recompile_run" events must have a "program_path" argument'})
                            continue

                        program_path = command.get('program_path')

                        try:
                            with _get_script_path(open(program_path)) as script_path:
                                await hub.download(script_path)
                                await hub.start_user_program()
                        except FileNotFoundError:
                            await send_event(OutgoingEventType.precondition_violated,
                                             payload={'explanation': 'received file path is not valid'})
                    else:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub must be connected to run a program'})

                case IncomingEventType.run_stored:
                    if hub:
                        await hub.stop_user_program()
                        await hub.start_user_program()
                    else:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub must be connected to run a program'})

                case IncomingEventType.send_string:
                    if 'string' not in command:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={
                                             'explanation': '"send_string" events must have a "string" argument'})
                        continue

                    if hub:
                        hub.write_string(command.get('string'))

                    else:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub must be connected to send text to stdin'})

                case IncomingEventType.cancel_running_program:
                    if hub:
                        await hub.stop_user_program()
                    else:
                        await send_event(OutgoingEventType.precondition_violated,
                                         payload={'explanation': 'a hub must be connected to cancel a program'})

                case IncomingEventType.exit:
                    if hub:
                        if hub_monitor_tasks:
                            hub_monitor_tasks.cancel()
                        await hub.disconnect()
                    os._exit(0)

                case _:
                    print(f"Invalid command: {command}")

        except HubDisconnectError:
            await send_event(OutgoingEventType.hub_disconnected)
            if hub_monitor_tasks:
                hub_monitor_tasks.cancel()
                hub_monitor_tasks = None
            hub = None
        # mpy-cross returned a compiler error
        except CalledProcessError as e:
            await send_event(OutgoingEventType.compile_error, {'traceback': e.stderr.decode()})


def main():
    PybricksHubBLE.download_user_program = download_user_program_override
    PybricksHubUSB.download_user_program = download_user_program_override
    asyncio.run(main_loop())


if __name__ == "__main__":
    main()
