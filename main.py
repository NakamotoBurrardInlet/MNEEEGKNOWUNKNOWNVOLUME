import time
import json
import queue
import threading
import socket
from pathlib import Path
from typing import Dict, Any, List
import numpy as np
import mne
from skyfield.api import load

class TelemetryNodeEngine:
    """
    Asynchronous telemetry node engine featuring bounded queue backpressure,
    append-only ledger persistence, and multi-target oracle proxy emission
    across designated network endpoints and constellation tethers.
    """
    def __init__(
        self, 
        ledger_path: str = "node_ledger.json", 
        max_queue_size: int = 5000,
        node_tag: str = "mne-eeg-omega-silent-knowing-the-unknown",
        bind_ip: str = "0.0.0.0",
        oracle_targets: List[str] = None,
        oracle_port: int = 9000
    ):
        self.ledger_path = Path(ledger_path)
        self.node_tag = node_tag
        self.bind_ip = bind_ip
        self.oracle_targets = oracle_targets or ["150.0.0.0", "8.8.8.8", "1.1.1.1", "2.2.2.2"]
        self.oracle_port = oracle_port
        
        self.emission_queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self.persistence_queue: queue.Queue = queue.Queue(maxsize=max_queue_size * 2)
        
        self._running = threading.Event()
        self._running.set()
        
        self._setup_oracle_sockets()
        
        self.emitter_thread = threading.Thread(target=self._emission_worker, daemon=True)
        self.ledger_thread = threading.Thread(target=self._persistence_worker, daemon=True)
        
        self.emitter_thread.start()
        self.ledger_thread.start()

    def _setup_oracle_sockets(self) -> None:
        self.sockets = []
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.bind_ip, 0))
            sock.setblocking(False)
            self.sockets.append(sock)
        except Exception:
            pass

    def capture_telemetry(self, payload: Dict[str, Any]) -> bool:
        if not self._running.is_set():
            return False

        timestamped_payload = {
            "node_signature": self.node_tag,
            "timestamp": time.time_ns(),
            "oracle_proxy_routes": self.oracle_targets,
            **payload
        }

        try:
            self.emission_queue.put(timestamped_payload, timeout=0.05)
            self.persistence_queue.put(timestamped_payload, timeout=0.05)
            return True
        except queue.Full:
            return False

    def _emission_worker(self) -> None:
        while self._running.is_set():
            try:
                payload = self.emission_queue.get(timeout=0.1)
                self._dispatch_oracle_network(payload)
                self.emission_queue.task_done()
            except queue.Empty:
                continue
            except Exception:
                pass

    def _persistence_worker(self) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        
        while self._running.is_set() or not self.persistence_queue.empty():
            batch = []
            try:
                item = self.persistence_queue.get(timeout=0.2)
                batch.append(item)
                self.persistence_queue.task_done()
                
                while len(batch) < 100:
                    try:
                        next_item = self.persistence_queue.get_nowait()
                        batch.append(next_item)
                        self.persistence_queue.task_done()
                    except queue.Empty:
                        break
                
                with open(self.ledger_path, "a", encoding="utf-8") as f:
                    for record in batch:
                        f.write(json.dumps(record) + "\n")
            except queue.Empty:
                continue
            except Exception:
                pass

    def _dispatch_oracle_network(self, payload: Dict[str, Any]) -> None:
        serialized = json.dumps(payload).encode("utf-8")
        for sock in self.sockets:
            for target in self.oracle_targets:
                try:
                    sock.sendto(serialized, (target, self.oracle_port))
                except (BlockingIOError, OSError):
                    pass

    def shutdown(self) -> None:
        self._running.clear()
        try:
            self.emission_queue.join()
            self.persistence_queue.join()
        except Exception:
            pass
            
        for sock in self.sockets:
            try:
                sock.close()
            except Exception:
                pass
                
        self.emitter_thread.join(timeout=2.0)
        self.ledger_thread.join(timeout=2.0)


def run_omega_constellation_pipeline():
    """
    Integrated MNE EEG sequencing and Skyfield orbital constellation telemetry 
    transistoring mechanism with buffering to resolve filter length warnings, 
    live binary/hexadecimal byte formatting, and persistent JSON ledger output.
    """
    node = TelemetryNodeEngine(
        ledger_path="omega_silent_knowing_ledger.jsonl",
        node_tag="mne-eeg-omega-silent-knowing-the-unknown",
        bind_ip="0.0.0.0",
        oracle_targets=["150.0.0.0", "8.8.8.8", "1.1.1.1", "2.2.2.2"],
        oracle_port=9000
    )

    info = mne.create_info(ch_names=['Fp1', 'Fp2', 'C3', 'C4', 'O1', 'O2'], sfreq=250.0, ch_types='eeg')
    ts = load.timescale()
    
    # Buffer to accumulate enough samples (1 second = 250 samples) to prevent MNE filter distortion warnings
    sfreq = int(info['sfreq'])
    buffer_window = []
    
    print("Initializing Omega Silent Knowing Telemetry & Constellation Pipeline...")
    
    try:
        chunk_duration = 0.1
        n_channels = len(info['ch_names'])
        chunk_samples = int(chunk_duration * sfreq)

        while True:
            start_time = time.time()
            t = ts.now()
            
            # Generate raw EEG chunk
            chunk_data = np.random.randn(n_channels, chunk_samples) * 1e-6
            buffer_window.append(chunk_data)
            
            # Accumulate until we have at least 1 full second (sfreq samples) for clean filtering
            total_samples = sum(arr.shape[1] for arr in buffer_window)
            if total_samples >= sfreq:
                combined_data = np.concatenate(buffer_window, axis=1)
                buffer_window.clear()
                
                # Construct MNE RawArray sequence for filtering
                raw = mne.io.RawArray(combined_data, info, verbose=False)
                raw.filter(l_freq=1.0, h_freq=40.0, fir_design='firwin', verbose=False)
                
                data_matrix = raw.get_data()
                channel_means = data_matrix.mean(axis=1).tolist()
                channel_stds = data_matrix.std(axis=1).tolist()
                
                payload = {
                    "pipeline": "mne-eeg-omega-constellation-transistor",
                    "constellation_utc": t.utc_iso(),
                    "channels": info['ch_names'],
                    "grey_matter_metrics": {
                        "means": channel_means,
                        "stds": channel_stds
                    },
                    "emitter_status": "silent_communicating_active"
                }

                # Serialize payload and generate binary and hexadecimal representations
                serialized = json.dumps(payload).encode("utf-8")
                hex_output = serialized.hex()
                bin_output = " ".join(format(b, '08b') for b in serialized[:32]) + ("..." if len(serialized) > 32 else "")

                print(f"\n[EMITTER TRANSMISSION] UTC: {t.utc_iso()}")
                print(f"  HEX : {hex_output[:64]}...")
                print(f"  BIN : {bin_output}")

                node.capture_telemetry(payload)

            elapsed = time.time() - start_time
            if elapsed < chunk_duration:
                time.sleep(chunk_duration - elapsed)

    except KeyboardInterrupt:
        print("\nDisengaging Omega pipeline...")
    finally:
        node.shutdown()

if __name__ == "__main__":
    run_omega_constellation_pipeline()
          
