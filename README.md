## Main Files

### V2ray — All Configs

| File | Link |
|---|---|
| All configs (txt) | [all_configs.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/all_configs.txt) |

### V2ray — By Protocol

| Protocol | Count | Link |
|---|---|---|
| VLESS | 897 | [vless.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vless.txt) |
| VMESS | 86 | [vmess.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vmess.txt) |
| SS | 467 | [ss.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/ss.txt) |
| TROJAN | 109 | [trojan.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/trojan.txt) |

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
| Batch 003 | 500 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_003.txt) |
| Batch 004 | 59 | [batch_004.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_004.txt) |

### Clash Batches

| Batch | Link |
|---|---|
| Batch 001 | [batch_001.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_001.yaml) |
| Batch 002 | [batch_002.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_002.yaml) |
| Batch 003 | [batch_003.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_003.yaml) |
| Batch 004 | [batch_004.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_004.yaml) |

### SNI Configs (server=127.0.0.1, port=40443)

> These configs have the server address replaced with `127.0.0.1:40443`.
> Use them with a local tunnel or gateway.
| File | Link |
|---|---|
| All SNI configs (txt) | [all_configs_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/all_configs_sni.txt) |
| clash_sni.yaml (all protocols) | [clash_sni.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/clash_sni.yaml) |
| clash_advanced_sni.yaml | [clash_advanced_sni.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/clash_advanced_sni.yaml) |

### SNI V2ray Batches

| Batch | Count | Link |
|---|---|---|
| Batch 001 | 500 | [batch_001.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_001.txt) |
| Batch 002 | 500 | [batch_002.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_002.txt) |
| Batch 003 | 500 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_003.txt) |
| Batch 004 | 59 | [batch_004.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_004.txt) |

#### SNI — By Protocol

| Protocol | Count | V2ray |
|---|---|---|
| VLESS | 897 | [vless_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vless_sni.txt) |
| VMESS | 86 | [vmess_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vmess_sni.txt) |
| SS | 467 | [ss_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/ss_sni.txt) |
| TROJAN | 109 | [trojan_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/trojan_sni.txt) |

---

## Statistics

### Per-Protocol Input & Output

| Protocol | Tested (unique) | valid | Pass Rate |
|---|---|---|---|
| VLESS | 3104 | 897 | 28.9% |
| VMESS | 629 | 86 | 13.7% |
| SS | 1368 | 467 | 34.1% |
| TROJAN | 438 | 109 | 24.9% |
| SSR | 9 | 0 | 0.0% |
| HY2 | 0 | 0 | 0.0% |
| HY | 0 | 0 | 0.0% |
| TUIC | 1 | 0 | 0.0% |
| **Total** | **5549** | **1559** | **28.1%** |

| Metric | Value |
|---|---|
| Raw fetched lines | 13003 |
| Unique after dedup | 5549 |
| Valid configs | 1559 |
| Processing time | 306.34s |

---

## 🔥 Keep This Project Going!

If you're finding this useful, please show your support:

⭐ **Star the repository on GitHub**

⭐ **Star our [Telegram posts](https://t.me/DeltaKroneckerGithub)** 

Your stars fuel our motivation to keep improving!
