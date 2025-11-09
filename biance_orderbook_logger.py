import websocket
import requests
import json
import threading
import time
import logging
from collections import deque

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

class BinanceOrderBookTracker:
    """
    This class connects to Binance, synchronizes the order book,
    and stores the initial snapshot and subsequent diffs in a file.
    
    It follows the synchronization logic specified by Binance:
    1. Subscribe to the diff stream.
    2. Buffer incoming diff events.
    3. Fetch a full snapshot from the REST API.
    4. Find the first diff event that follows the snapshot.
    5. Process all subsequent diffs, checking for gaps.
    6. Write the snapshot and all valid diffs to a file.
    """
    
    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        
        # API Endpoints
        self.snapshot_url = "https://api.binance.com/api/v3/depth"
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol_lower}@depth"
        
        # State variables
        self.ws = None
        self.ws_thread = None
        self.snapshot_last_update_id = None
        self.last_final_update_id = None  # U from the last processed event
        self.diff_buffer = deque()
        self.snapshot_ready = threading.Event() # Use an event to signal snapshot readiness
        self.is_running = True

        # Output file
        self.output_filename = f"{self.symbol_lower}_diffs.jsonl"
        self.output_file = open(self.output_filename, "w")
        log.info(f"Will write data to {self.output_filename}")

    def _on_open(self, ws):
        log.info("WebSocket connection opened.")
        # Subscription is now handled in the start method to align with snapshot logic
        pass

    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        data = json.loads(message)
        
        # Check for depthUpdate event
        if data.get("e") == "depthUpdate":
            # If snapshot isn't ready, buffer the diff
            if not self.snapshot_ready.is_set():
                self.diff_buffer.append(data)
                # log.debug(f"Buffering diff {data.get('u')}-{data.get('U')}")
            else:
                # Once snapshot is ready, process live
                self._process_diff(data)
        elif "result" in data and data.get("id") == 1:
            log.info(f"Subscription successful: {data['result']}")
        else:
            log.debug(f"Received other message: {data}")

    def _on_error(self, ws, error):
        log.error(f"WebSocket Error: {error}")
        self.is_running = False

    def _on_close(self, ws, close_status_code, close_msg):
        log.info("WebSocket connection closed.")
        self.is_running = False

    def _get_initial_snapshot(self):
        """Fetches the initial order book snapshot from the REST API."""
        try:
            log.info(f"Fetching initial snapshot for {self.symbol} (limit 1000)...")
            params = {"symbol": self.symbol, "limit": 1000}
            response = requests.get(self.snapshot_url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            snapshot = response.json()
            self.snapshot_last_update_id = snapshot['lastUpdateId']
            
            # Save the snapshot to the file
            snapshot_data = {"type": "snapshot", "timestamp": time.time(), "data": snapshot}
            self.output_file.write(json.dumps(snapshot_data) + '\n')
            self.output_file.flush()
            
            log.info(f"Snapshot received. LastUpdateId: {self.snapshot_last_update_id}")

        except requests.exceptions.RequestException as e:
            log.error(f"Error fetching snapshot: {e}")
            self.stop() # Stop if we can't get the snapshot

    def _process_buffer(self):
        """Process the diffs that were buffered during snapshot fetching."""
        log.info(f"Processing {len(self.diff_buffer)} buffered diffs...")
        while self.diff_buffer:
            diff = self.diff_buffer.popleft()
            self._process_diff(diff)
        log.info("Buffer processed. Now processing live diffs.")

    def _process_diff(self, data):
        """
        Processes a single diff event, checking for synchronization and saving it.
        """
        U_first = data['U']  # First update ID in event
        u_final = data['u']  # Final update ID in event
        
        # 1. Skip any events where U <= snapshot's lastUpdateId
        if u_final <= self.snapshot_last_update_id:
            # log.debug(f"Skipping old diff {u_first}-{U_final}")
            return
            
        # 2. Check if this is the first event to process
        if self.last_final_update_id is None:
            # Binance rule: first event's 'u' must be <= snapshot's 'lastUpdateId + 1'
            # AND its 'U' must be >= snapshot's 'lastUpdateId + 1'
            if not (U_first <= self.snapshot_last_update_id + 1 and u_final >= self.snapshot_last_update_id + 1):
                log.error("Snapshot and first diff are out of sync. Resynchronizing...")
                self._resync()
                return
            else:
                log.info(f"First valid diff processed: {U_first}-{u_final}")

        # 3. Check for gaps in subsequent events
        else:
            # Current event's 'u' must be exactly 'last_final_update_id + 1'
            if U_first != self.last_final_update_id + 1:
                log.error(f"Gap in diffs! Expected {self.last_final_update_id + 1}, got {U_first}. Resynchronizing...")
                self._resync()
                return

        # If we're here, the diff is valid. Save it.
        diff_data = {"type": "diff", "data": data}
        self.output_file.write(json.dumps(diff_data) + '\n')
        
        # Update the last final update ID
        self.last_final_update_id = u_final

    def _run_websocket(self):
        """Runs the WebSocket client in a loop."""
        log.info("Starting WebSocket thread...")
        self.ws = websocket.WebSocketApp(self.ws_url,
                                         on_open=self._on_open,
                                         on_message=self._on_message,
                                         on_error=self._on_error,
                                         on_close=self._on_close)
        
        # We need to manually send subscription *after* on_open
        def on_open_wrapper(ws):
            self._on_open(ws)
            # Subscribe *after* connection is open
            log.info(f"Subscribing to {self.symbol_lower}@depth...")
            ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": [f"{self.symbol_lower}@depth"],
                "id": 1
            }))
        
        self.ws.on_open = on_open_wrapper
        
        while self.is_running:
            try:
                self.ws.run_forever(ping_interval=60, ping_timeout=10)
            except Exception as e:
                log.error(f"WebSocket run_forever() failed: {e}. Retrying in 5s...")
            if self.is_running:
                time.sleep(5)
        log.info("WebSocket thread finished.")

    def _resync(self):
        """Handles a resynchronization by restarting the process."""
        log.warning("Resync triggered.")
        # Stop the current WebSocket
        if self.ws:
            self.ws.close()
        
        # Clear state
        self.snapshot_ready.clear()
        self.last_final_update_id = None
        self.diff_buffer.clear()
        
        # Relaunch the process
        self.start_processes()

    def start(self):
        """Starts the tracking process."""
        self.start_processes()
        
        # Keep main thread alive to catch KeyboardInterrupt
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt received. Stopping...")
            self.stop()
            
    def start_processes(self):
        """The core logic of starting WebSocket and fetching snapshot."""
        
        # 1. Start WebSocket thread (it will connect and start buffering)
        self.ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
        self.ws_thread.start()
        
        # Give websocket a moment to connect and start buffering
        time.sleep(2) 
        
        # 2. Fetch the snapshot
        self._get_initial_snapshot()
        
        # 3. Signal that snapshot is ready
        self.snapshot_ready.set()
        
        # 4. Process any buffered diffs
        self._process_buffer()

    def stop(self):
        """Stops the tracker and cleans up resources."""
        self.is_running = False
        
        if self.ws:
            self.ws.close()
            
        if self.ws_thread:
            self.ws_thread.join()
            
        if self.output_file:
            self.output_file.close()
            log.info("Output file closed.")
            
        log.info("Tracker stopped.")

if __name__ == "__main__":
    # Start the tracker for BTC/USDT
    tracker = BinanceOrderBookTracker(symbol="BTCUSDT")
    tracker.start()