"""nbclient 없이 jupyter_client 로 노트북을 실행하고 출력을 채워 넣는다."""
import json
import sys

from jupyter_client.manager import start_new_kernel

path, cwd = sys.argv[1], sys.argv[2]
nb = json.load(open(path, encoding="utf-8"))
km, kc = start_new_kernel(kernel_name="python3", cwd=cwd)
n, failed = 0, False
try:
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        n += 1
        msg_id = kc.execute("".join(cell["source"]))
        outputs = []
        while True:
            msg = kc.get_iopub_msg(timeout=1800)
            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            t, c = msg["msg_type"], msg["content"]
            if t == "status" and c["execution_state"] == "idle":
                break
            if t == "stream":
                if outputs and outputs[-1]["output_type"] == "stream" and outputs[-1]["name"] == c["name"]:
                    outputs[-1]["text"] += c["text"]
                else:
                    outputs.append({"output_type": "stream", "name": c["name"], "text": c["text"]})
            elif t == "execute_result":
                outputs.append({"output_type": t, "data": c["data"], "metadata": c["metadata"], "execution_count": n})
            elif t == "display_data":
                outputs.append({"output_type": t, "data": c["data"], "metadata": c["metadata"]})
            elif t == "error":
                outputs.append({"output_type": "error", "ename": c["ename"], "evalue": c["evalue"],
                                "traceback": c["traceback"]})
                failed = True
        for o in outputs:
            if o["output_type"] == "stream":
                o["text"] = o["text"].splitlines(keepends=True)
        cell["outputs"], cell["execution_count"] = outputs, n
        err = next((o["ename"] + ": " + o["evalue"] for o in outputs if o["output_type"] == "error"), None)
        print(f"cell {n}: {'ERROR ' + err if failed else 'ok'}", flush=True)
        if failed:
            break
finally:
    kc.stop_channels()
    km.shutdown_kernel(now=True)
json.dump(nb, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
sys.exit(1 if failed else 0)
