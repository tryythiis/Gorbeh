## SNI Configs

> If you don't know what SNI-Spoofing configurations are for, skip this table.
> [https://t.me/DeltaSNI](https://t.me/DeltaSNI) برای آموزش و گفت و گو درباره روش های مبتنی بر این روش در گروه تلگرامی ما عضو بشید
> لطفا هر پروژه ای براتون مفید بود حتما استار بدید، با این کار انگیزه توسعه دهنده برای ادامه رو تامین می کنید🫀

| Type | Count | Link |
|---|---|---|
| All SNI configs | — | [all_configs_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/all_configs_sni.txt) |
| Protocol: VLESS | 825 | [vless_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vless_sni.txt) |
| Protocol: VMESS | 31 | [vmess_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/vmess_sni.txt) |
| Protocol: SS | 200 | [ss_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/ss_sni.txt) |
| Protocol: TROJAN | 92 | [trojan_sni.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/sni/protocols/trojan_sni.txt) |
| Batch 001 | 500 | [batch_001.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_001.txt) |
| Batch 002 | 500 | [batch_002.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_002.txt) |
| Batch 003 | 148 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/sni_v2ray/batch_003.txt) |

---

## Main Files

### V2ray — All Configs

| File | Link |
|---|---|
| All configs (txt) | [all_configs.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/all_configs.txt) |

### V2ray — By Protocol

| Protocol | Count | Link |
|---|---|---|
| VLESS | 825 | [vless.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vless.txt) |
| VMESS | 31 | [vmess.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/vmess.txt) |
| SS | 200 | [ss.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/ss.txt) |
| TROJAN | 92 | [trojan.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/protocols/trojan.txt) |

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
| Batch 003 | 148 | [batch_003.txt](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/v2ray/batch_003.txt) |

### Clash Batches

| Batch | Link |
|---|---|
| Batch 001 | [batch_001.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_001.yaml) |
| Batch 002 | [batch_002.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_002.yaml) |
| Batch 003 | [batch_003.yaml](https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main/config/batches/clash/batch_003.yaml) |

---

## Statistics

### Per-Protocol Input & Output

| Protocol | Tested (unique) | valid | Pass Rate |
|---|---|---|---|
| VLESS | 2374 | 825 | 34.8% |
| VMESS | 440 | 31 | 7.0% |
| SS | 295 | 200 | 67.8% |
| TROJAN | 292 | 92 | 31.5% |
| SSR | 9 | 0 | 0.0% |
| HY2 | 0 | 0 | 0.0% |
| HY | 0 | 0 | 0.0% |
| TUIC | 1 | 0 | 0.0% |
| **Total** | **3411** | **1148** | **33.7%** |

| Metric | Value |
|---|---|
| Raw fetched lines | 4553 |
| Unique after dedup | 3411 |
| Valid configs | 1148 |
| Processing time | 192.38s |

---

## 🔥 Keep This Project Going!

If you're finding this useful, please show your support with:

⭐ **Star the repository**

Your stars fuel our motivation to keep improving!
