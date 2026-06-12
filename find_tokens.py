"""Поиск живых токенов по площадкам: TopBlast (new factory, get_meme_data),
Uranus (legacy shared factory, get_meme_data), Stonks (get_bonding_data).
Печатает friendly-CA. Запуск: .venv/Scripts/python.exe find_tokens.py"""
import asyncio
import config
from ton_source import TonSource, _to_friendly

try:
    from pytoniq_core import Address
    def friendly(raw): return Address(raw).to_str(is_user_friendly=True, is_bounceable=True)
except Exception:
    def friendly(raw): return raw

TOPBLAST_NEW = "EQA6ivhIOBQJvqO1SY3IvutmluM1d817gZDeEjiaVNBLePd3"
URANUS_LEGACY = "EQAO4cYqithwdltzmrlal1L5JKLK5Xk76feAJq0VoBC6Fy8T"
STONKS = "EQDEcx-hDfCH4ktFKAaz_PV7lqIlv1R38oCPEyar8DOE0zCs"


def extract(obj, fact, out=None):
    out = out if out is not None else set()
    if isinstance(obj, dict):
        a = obj.get("address")
        if a and obj.get("is_wallet") is False and a not in config.FACTORY_TO_PLATFORM:
            out.add(a)
        for v in obj.values():
            extract(v, fact, out)
    elif isinstance(obj, list):
        for v in obj:
            extract(v, fact, out)
    return out


async def find(ton, factory, model, n=2):
    data = await ton._ta_get(f"/accounts/{factory}/events", {"limit": 60})
    found = []
    for cand in extract(data, factory):
        if model == "meme":
            md = await ton.get_meme_data(cand)
            ok = md is not None
        else:
            ok = await ton.read_bonding(cand) is not None
        if ok:
            meta = await ton.get_meta(cand)
            found.append((friendly(cand), meta["symbol"], meta["name"]))
            if len(found) >= n:
                break
    return found


async def main():
    ton = TonSource()
    for label, fact, model in [
        ("TOPBLAST", TOPBLAST_NEW, "meme"),
        ("URANUS  ", URANUS_LEGACY, "meme"),
        ("STONKS  ", STONKS, "bonding"),
    ]:
        res = await find(ton, fact, model)
        print(f"=== {label} ===")
        for ca, sym, name in res:
            print(f"  {ca}  ${sym}  {name}")
        if not res:
            print("  (не найдено в последних событиях)")
    await ton.close()


if __name__ == "__main__":
    asyncio.run(main())
