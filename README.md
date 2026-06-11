# BrickPipe

Brickpipe provides a JSON-based Inter-Process Communication (IPC) interface to interact with Pybricks hubs. It
communicates via standard input (`stdin`) for commands and standard output (`stdout`) for events.

Each message (incoming or outgoing) must be a single-line JSON object.

## Message Format

All messages are JSON objects that contain an `event_type` field.

```json
{
  "event_type": "event_name",
  "other_field": "value"
}
```

---

## Incoming Events (Commands)

These commands are sent to the script via `stdin`.

| `event_type`             | Parameters                                                                                                          | Description                                                                                          |
|:-------------------------|:--------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------|
| `start_ble_scanning`     | `timeout` : int \| float (required)                                                                                 | Begins bluetooth scanning with a specified timeout.                                                  |
| `connect_to_hub`         | `conn_type`: `"ble"` or `"usb"` (required)<br>`ble_address`: string (optional)<br>`ble_hub_name`: string (optional) | Connects to a hub. `ble_address` and `ble_hub_name` are used for filtering bluetooth hubs.           |
| `disconnect_from_hub`    | None                                                                                                                | Disconnects the currently connected hub.                                                             |
| `recompile_download`     | `program_path`: string (required)                                                                                   | Stops any running program, recompiles the specified Python file, and downloads it to the hub.        |
| `recompile_run`          | `program_path`: string (required)                                                                                   | Stops any running program, recompiles, downloads, and then starts the program.                       |
| `run_stored`             | None                                                                                                                | Stops any running program and starts the program already stored in the hub.                          |
| `send_string`            | `string`: string (required)                                                                                         | Sends a string to the hub's `stdin`.                                                                 |
| `cancel_running_program` | None                                                                                                                | Stops the user program currently running on the hub. This has no effect if a program is not running. |
| `exit`                   | None                                                                                                                | Disconnects any active hub and terminates the process.                                               |

---

## Outgoing Events

These events are sent from the script via `stdout`.

| `event_type`               | Payload Fields                                              | Description                                                                            |
|:---------------------------|:------------------------------------------------------------|:---------------------------------------------------------------------------------------|
| `ble_device_found`         | `device_name`: string<br/>`address`: string<br/>`rssi`: int | Found a hub from scanning. Also triggered by an `rssi` update when scanning.           |
| `hub_connected`            | None                                                        | Successfully established a connection with a hub.                                      |
| `connection_timeout`       | None                                                        | Failed to find or connect to a hub.                                                    |
| `download_progress_update` | `percentage`: float (0.00 - 100.00)                         | Indicates the current progress when downloading a program onto the hub.                |
| `program_started`          | None                                                        | A user program has started running on the hub.                                         |
| `program_complete`         | None                                                        | The user program has finished running or was stopped.                                  |
| `hub_printed_line`         | `line`: string                                              | A line of text output from the hub (stdout).                                           |
| `compile_error`            | `traceback`: string                                         | The Python script failed to compile. Contains the error details.                       |
| `precondition_violated`    | `explanation`: string                                       | The command could not be executed (e.g., missing arguments, or hub not connected).     |
| `hub_firmware_outdated`    | `explanation`: string                                       | The hub's firmware version is not supported. Can only appear when connecting to a hub. |
| `hub_disconnected`         | None                                                        | The hub has been disconnected (either manually or due to an error).                    |

---

## Example Usage

### Connecting and Running a Script

**1. Connect via bluetooth:**

- **Input:** `{"event_type": "connect_to_hub", "conn_type": "ble", "hub_name": "Robot"}`
- **Output:** `{"event_type": "hub_connected"}`

**2. Download and Run a Program:**

- **Input:** `{"event_type": "recompile_run", "program_path": "main.py"}`
- **Output:**
    - `{"event_type": "download_progress_update", "percentage": 50.0}`
    - `{"event_type": "download_progress_update", "percentage": 100.0}`
    - `{"event_type": "program_started"}`
    - `{"event_type": "hub_printed_line", "line": "Hello, World!"}`
    - `{"event_type": "program_complete"}`

**3. Handling a Compile Error:**

- **Input:** `{"event_type": "recompile_run", "program_path": "broken.py"}`
- **Output:** `{"event_type": "compile_error", "traceback": "..."}`