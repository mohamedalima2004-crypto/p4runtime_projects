#!/usr/bin/env python3
"""
simple_control.py

P4Runtime controller:
 - open StreamChannel and perform MasterArbitration (keeps stream alive)
 - set forwarding pipeline (BMv2 JSON + P4Info)
 - insert two LPM table entries for ipv4 forwarding

Usage:
  python3 simple_control.py \
    --p4info ~/build/test.p4info.txt \
    --json  ~/build/test.json \
    --addr 127.0.0.1:50051 \
    --device_id 0 \
    --election_id 10
"""
import argparse
import grpc
import time
from google.protobuf import text_format
from p4.v1 import p4runtime_pb2, p4runtime_pb2_grpc
from p4.config.v1 import p4info_pb2


# -------- Helpers --------
def ipv4_to_bytes(ip):
    """Convert dotted IPv4 string into 4-byte value."""
    return bytes(map(int, ip.split(".")))


def load_p4info(path):
    p4info = p4info_pb2.P4Info()
    with open(path, "r") as f:
        text_format.Parse(f.read(), p4info)
    return p4info


def load_json(path):
    with open(path, "rb") as f:
        return f.read()


# -------- Main --------
def main():
    parser = argparse.ArgumentParser(description="P4Runtime Controller")
    parser.add_argument("--p4info", required=True, help="Path to p4info file (text)")
    parser.add_argument("--json", required=True, help="Path to BMv2 JSON file")
    parser.add_argument("--addr", default="127.0.0.1:50051", help="P4Runtime gRPC address")
    parser.add_argument("--device_id", type=int, default=0, help="Target device ID")
    parser.add_argument("--election_id", type=int, default=10, help="Election ID for arbitration")
    args = parser.parse_args()

    # connect
    channel = grpc.insecure_channel(args.addr)
    stub = p4runtime_pb2_grpc.P4RuntimeStub(channel)

    # ---------- Arbitration: open StreamChannel and keep it alive ----------
    # Generator that sends an initial MasterArbitrationUpdate with election_id,
    # then periodically yields empty messages to keep the stream alive.
    def stream_requests():
        req = p4runtime_pb2.StreamMessageRequest()
        req.arbitration.device_id = args.device_id
        req.arbitration.election_id.high = 0
        req.arbitration.election_id.low = args.election_id
        yield req

        # keep the stream alive (periodically re-send arbitration to reinforce)
        while True:
            # sleep a short while to avoid busy-loop
            time.sleep(2)
            keep = p4runtime_pb2.StreamMessageRequest()
            # Optionally re-send election_id to keep master alive
            keep.arbitration.device_id = args.device_id
            keep.arbitration.election_id.high = 0
            keep.arbitration.election_id.low = args.election_id
            yield keep

    stream = stub.StreamChannel(stream_requests())

    # Wait for the switch reply to arbitration
    try:
        arb_resp = next(stream)  # blocking until server replies
    except grpc.RpcError as e:
        print("[ERROR] StreamChannel failed during arbitration:", e)
        return

    # The response should contain arbitration result
    if arb_resp.WhichOneof("update") != "arbitration":
        print("[ERROR] Unexpected StreamChannel response (no arbitration).")
        return

    status = arb_resp.arbitration.status
    if status.code != 0:
        # Not primary
        print("[ERROR] Arbitration failed. Not primary. Status:", status)
        return

    # Success - we are primary
    print(f"[INFO] ✅ Became primary controller (election_id={args.election_id})")

    # ---------- Load P4Info and BMv2 JSON ----------
    try:
        p4info = load_p4info(args.p4info)
    except Exception as e:
        print("[ERROR] Failed to load p4info:", e)
        return

    try:
        device_config = load_json(args.json)
    except Exception as e:
        print("[ERROR] Failed to load BMv2 JSON:", e)
        return

    # ---------- Set Forwarding Pipeline ----------
    set_req = p4runtime_pb2.SetForwardingPipelineConfigRequest()
    set_req.device_id = args.device_id
    set_req.action = p4runtime_pb2.SetForwardingPipelineConfigRequest.VERIFY_AND_COMMIT
    set_req.election_id.high = 0
    set_req.election_id.low = args.election_id
    set_req.config.p4info.CopyFrom(p4info)
    set_req.config.p4_device_config = device_config

    try:
        print("[INFO] Installing pipeline config...")
        stub.SetForwardingPipelineConfig(set_req)
        print("[INFO] ✅ Pipeline config installed")
    except grpc.RpcError as e:
        print("[ERROR] SetForwardingPipelineConfig failed:", e)
        return

    # ---------- Lookup table/action IDs from P4Info ----------
    table_name = "my_ingress.ipv4_match"
    action_name = "my_ingress.to_port_action"
    match_field = "hdr.ipv4.dst_addr"

    table_id = None
    match_field_id = None
    action_id = None
    param_id = None

    for t in p4info.tables:
        if t.preamble.name == table_name:
            table_id = t.preamble.id
            for mf in t.match_fields:
                if mf.name == match_field:
                    match_field_id = mf.id
            break

    for a in p4info.actions:
        if a.preamble.name == action_name:
            action_id = a.preamble.id
            # find the param id named "port" if present
            for p in a.params:
                if p.name == "port":
                    param_id = p.id
                    break
            # fallback to first param if no explicit "port" name
            if param_id is None and a.params:
                param_id = a.params[0].id
            break

    if not all([table_id, match_field_id, action_id, param_id]):
        print("[ERROR] Could not find required IDs in P4Info:")
        print(" table_id:", table_id, "match_field_id:", match_field_id,
              "action_id:", action_id, "param_id:", param_id)
        return

    # ---------- Helper to insert LPM rule ----------
    def insert_lpm(dst_ip, prefix_len, out_port):
        te = p4runtime_pb2.TableEntry()
        te.table_id = table_id

        fm = te.match.add()
        fm.field_id = match_field_id
        fm.lpm.value = ipv4_to_bytes(dst_ip)
        fm.lpm.prefix_len = prefix_len

        te.action.action.action_id = action_id
        p = te.action.action.params.add()
        p.param_id = param_id
        # convert integer port into bytes (use 2 bytes as earlier)
        p.value = out_port.to_bytes(2, "big")

        upd = p4runtime_pb2.Update()
        upd.type = p4runtime_pb2.Update.INSERT
        upd.entity.table_entry.CopyFrom(te)

        write_req = p4runtime_pb2.WriteRequest()
        write_req.device_id = args.device_id
        write_req.election_id.high = 0
        write_req.election_id.low = args.election_id
        write_req.updates.extend([upd])

        try:
            stub.Write(write_req)
            print(f"[INFO] Inserted {dst_ip}/{prefix_len} -> port {out_port}")
        except grpc.RpcError as e:
            print("[ERROR] Write RPC failed:", e)

    # Insert example entries (adjust ports to match your BMv2 mapping)
    # Note: BMv2 ports are the numbers you passed to -i when launching simple_switch_grpc
    insert_lpm("10.0.0.1", 32, 0)  # forward to port 0 (veth1)
    insert_lpm("10.0.0.2", 32, 1)  # forward to port 1 (veth3)

    print("[INFO] ✅ All entries inserted. Controller will keep running to hold mastership.")
    try:
        # keep process alive (and StreamChannel generator is running) so we remain primary
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Exiting controller (will release mastership).")


if __name__ == "__main__":
    main()

