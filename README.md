## SNI Configs (server=127.0.0.1, port=40443)

> These configs have the server address replaced with `127.0.0.1:40443`.
> Use them with a local tunnel or gateway that forwards to the real server.
> All TLS SNI/peer fields are preserved so TLS handshake still works correctly.

| File | Link |
|---|---|
| All SNI configs (txt) | [all_configs_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/all_configs_sni.txt) |

#### SNI — By Protocol

| Protocol | Count | V2ray SNI Config |
|---|---|---|
| VLESS | 814 | [vless_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vless_sni.txt) |
| VMESS | 27 | [vmess_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vmess_sni.txt) |
| SS | 194 | [ss_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/ss_sni.txt) |
| TROJAN | 91 | [trojan_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/trojan_sni.txt) |

---

## Main Files

### V2ray — All Configs

| File | Link |
|---|---|
| All configs (txt) | [all_configs.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/all_configs.txt) |

### V2ray — By Protocol

| Protocol | Count | Link |
|---|---|---|
| VLESS | 814 | [vless.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vless.txt) |
| VMESS | 27 | [vmess.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vmess.txt) |
| SS | 194 | [ss.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/ss.txt) |
| TROJAN | 91 | [trojan.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/trojan.txt) |

### Clash 

Groups: **PROXY** (selector) → **Load-Balance** · **Auto** · **Fallback**

| File | Link |
|---|---|
| clash.yaml (all protocols) | [clash.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/clash.yaml) |
| vless_clash.yaml | [vless_clash.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vless_clash.yaml) |
| vmess_clash.yaml | [vmess_clash.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vmess_clash.yaml) |
| ss_clash.yaml | [ss_clash.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/ss_clash.yaml) |
| trojan_clash.yaml | [trojan_clash.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/trojan_clash.yaml) |

---

## Batch Files — Random 500-Config Groups

> Each file contains 500 randomly selected configs from all protocols.

### V2ray Batches

| Batch | Count | Link |
|---|---|---|
| Batch 001 | 500 | [batch_001.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_001.txt) |
| Batch 002 | 500 | [batch_002.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_002.txt) |
| Batch 003 | 126 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_003.txt) |

### Clash Batches

| Batch | Link |
|---|---|
| Batch 001 | [batch_001.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_001.yaml) |
| Batch 002 | [batch_002.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_002.yaml) |
| Batch 003 | [batch_003.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_003.yaml) |

### SNI V2ray Batches

| Batch | Count | Link |
|---|---|---|
| Batch 001 | 500 | [batch_001.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_001.txt) |
| Batch 002 | 500 | [batch_002.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_002.txt) |
| Batch 003 | 126 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_003.txt) |

---

## Statistics

### Per-Protocol Input & Output

| Protocol | Tested (unique) | valid | Pass Rate |
|---|---|---|---|
| VLESS | 2369 | 814 | 34.4% |
| VMESS | 439 | 27 | 6.2% |
| SS | 295 | 194 | 65.8% |
| TROJAN | 266 | 91 | 34.2% |
| SSR | 9 | 0 | 0.0% |
| HY2 | 0 | 0 | 0.0% |
| HY | 0 | 0 | 0.0% |
| TUIC | 1 | 0 | 0.0% |
| **Total** | **3379** | **1126** | **33.3%** |

| Metric | Value |
|---|---|
| Raw fetched lines | 4546 |
| Unique after dedup | 3379 |
| Valid configs | 1126 |
| Processing time | 189.96s |

---

## 🔥 Keep This Project Going!

If you're finding this useful, please show your support:

⭐ **Star the repository on GitHub**

⭐ **Star our [Telegram posts](https://t.me/DeltaKroneckerGithub)** 

Your stars fuel our motivation to keep improving!
