"""Найти реальные TopBlast-токены по уровням сбора: 25+/50+/75+ и забондившийся.
Пагинируем события фабрик → собираем Meme-адреса → читаем get_meme_data →
прогресс = raised/target*100. Запуск: .venv/Scripts/python.exe find_progress.py"""
import asyncio
import config
from ton_source import TonSource

try:
    from pytoniq_core import Address
    def friendly(raw): return Address(raw).to_str(is_user_friendly=True, is_bounceable=True)
except Exception:
    def friendly(raw): return raw

# фабрика → (платформа, таргет TON, префиксы адресов токенов этой площадки)
FACTORIES = {
    "EQA6ivhIOBQJvqO1SY3IvutmluM1d817gZDeEjiaVNBLePd3": ("topblast", 1550.0, ("EQA6", "0:3a")),
    "EQAO4cYqithwdltzmrlal1L5JKLK5Xk76feAJq0VoBC6Fy8T": ("uranus", 1050.0, ("EQAO", "0:0e")),
}
PAGES = 70         # глубина пагинации на фабрику
sem = asyncio.Semaphore(12)

# известные активные адреса (seed), чтобы гарантированно попали в пул
SEED = {
    "EQA6ktdw3lsq6c6MnU_vCGNUwfaWyObV5sLtzcogWrb1Y-de": ("topblast", 1550.0),
    "EQA6iDIR4zMPLF0WphiC3e9TaM77kmZaqg9iMYimrpoXgKmv": ("topblast", 1550.0),
    "EQA6enm16bWjG1eUKI19E3EbWZjlB7r79RmpCTx79PymzgnK": ("topblast", 1550.0),
    "EQA6CAnpjW3EplOdZXrH4-MvuoL5L5GzKH-P6UJceMqPGHu3": ("topblast", 1550.0),
    "EQA69Mhu92OBCtAse0SJbv3_3VpiVV10d5vLUw_3Bg8Myt_0": ("topblast", 1550.0),
}


def extract(obj, prefixes, out):
    """Собираем ВСЕ адреса с префиксом площадки (get_meme_data — финальный фильтр).
    Без условия is_wallet — оно теряло мастера в части событий."""
    if isinstance(obj, dict):
        a = obj.get("address")
        if a and a not in config.FACTORY_TO_PLATFORM and str(a).startswith(prefixes):
            out.add(a)
        for v in obj.values():
            extract(v, prefixes, out)
    elif isinstance(obj, list):
        for v in obj:
            extract(v, prefixes, out)


async def collect_candidates(ton):
    """Возвращает ca → (platform, target) по фабрике-источнику."""
    cmap = {}
    for fact, (plat, target, prefixes) in FACTORIES.items():
        before_lt = None
        for _ in range(PAGES):
            params = {"limit": 100}
            if before_lt:
                params["before_lt"] = before_lt
            data = await ton._ta_get(f"/accounts/{fact}/events", params)
            evs = (data or {}).get("events") or []
            if not evs:
                break
            page = set()
            extract(evs, prefixes, page)
            for c in page:
                cmap_set(cmap, c, plat, target)
            before_lt = evs[-1].get("lt")
            if not before_lt:
                break
    return cmap


def cmap_set(cmap, ca, plat, target):
    cmap.setdefault(ca, (plat, target))


async def probe(ton, ca, plat, target):
    async with sem:
        md = await ton.get_meme_data(ca)
    if not md:
        return None
    raised = md["raised_funds"]
    return {
        "ca": ca, "platform": plat, "target": target, "raised": raised,
        "progress": min(raised / target * 100, 100.0),
        "graduated": md["is_graduated"] or md["migrated"],
    }


async def main():
    ton = TonSource()
    cmap = await collect_candidates(ton)
    print(f"кандидатов: {len(cmap)}")
    results = [r for r in await asyncio.gather(
        *(probe(ton, c, p, t) for c, (p, t) in cmap.items())) if r]
    live = [r for r in results if not r["graduated"]]
    grad = [r for r in results if r["graduated"]]
    live.sort(key=lambda r: r["progress"], reverse=True)

    def pick(lo, hi):  # лучший НЕ забондившийся в диапазоне
        xs = [r for r in live if lo <= r["progress"] < hi]
        return xs[0] if xs else None

    print(f"\nживых (не бонд): {len(live)}  ·  забондившихся: {len(grad)}")
    print("\nТОП-12 живых по сбору:")
    for r in live[:12]:
        sym = (await ton.get_meta(r["ca"]))["symbol"]
        print(f"  {r['progress']:6.2f}%  {r['raised']:7.1f}/{r['target']:.0f} TON  {r['platform']:<8} ${sym}  {friendly(r['ca'])}")

    print("\nВЫБОР ПО УРОВНЯМ:")
    chosen = [("25+%", pick(25, 50)), ("50+%", pick(50, 75)),
              ("75+%", pick(75, 100)), ("БОНД", grad[0] if grad else None)]
    for label, r in chosen:
        if r:
            sym = (await ton.get_meta(r["ca"]))["symbol"]
            print(f"  {label:5} -> {r['progress']:6.2f}%  {r['platform']:<8} ${sym}  {friendly(r['ca'])}")
        else:
            print(f"  {label:5} -> не найдено")
    await ton.close()


if __name__ == "__main__":
    asyncio.run(main())
