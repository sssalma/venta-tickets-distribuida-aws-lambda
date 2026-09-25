import sys, os, threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.postgres_repository import PostgresRepository
from base.tickets import tickets
from base.reset_postgres import reset_postgres


def create_repo():
    repo = PostgresRepository()
    repo.conectar()
    return repo


def close_repo(repo):
    if repo:
        repo.desconectar()


def test_numerada():
    reset_postgres()
    repo = create_repo()
    svc = tickets(repo)

    r1 = svc.comprar_numerada("c1", 42, "r1")
    assert r1.ok and r1.status == "COMPRADO"

    r2 = svc.comprar_numerada("c2", 42, "r2")
    assert not r2.ok and r2.status == "FAIL"
    print("  [OK] Numerada: 1ra exitosa, 2da mismo asiento falla")

    close_repo(repo)


def test_numerada_idempotencia():
    reset_postgres()
    repo = create_repo()
    svc = tickets(repo)

    first = svc.comprar_numerada("cA", 99, "r-idem")
    second = svc.comprar_numerada("cB", 99, "r-idem")

    assert second.ok == first.ok
    assert second.status == first.status
    assert second.motivo == first.motivo
    assert second.seat_id == first.seat_id
    print("  [OK] Idempotencia numerada: mismo request_id devuelve mismo resultado")

    close_repo(repo)


def test_unnumbered():
    reset_postgres()
    repo = create_repo()
    svc = tickets(repo)

    r1 = svc.comprar_no_numerada("c1", "r1")
    assert r1.ok and r1.status == "COMPRADO"

    r2 = svc.comprar_no_numerada("c2", "r2")
    assert r2.ok and r2.status == "COMPRADO"

    with repo.connection.cursor() as cur:
        cur.execute("SELECT sold_count FROM unnumbered_counter WHERE id = 1")
        sold_count = cur.fetchone()[0]
    assert sold_count == 2
    print("  [OK] Unnumbered: contador incrementa correctamente")

    close_repo(repo)


def test_unnumbered_sold_out():
    reset_postgres()
    repo = create_repo()
    with repo.connection.cursor() as cur:
        cur.execute("UPDATE unnumbered_counter SET sold_count = 100000 WHERE id = 1")
    repo.connection.commit()

    svc = tickets(repo)
    r = svc.comprar_no_numerada("cLO", "r-soldout")
    assert not r.ok and r.status == "FAIL"
    print("  [OK] Unnumbered sold_out: no hay tickets adicionales")

    close_repo(repo)


def test_concurrencia():
    reset_postgres()
    resultados = []
    lock = threading.Lock()

    def comprar(i):
        thread_repo = PostgresRepository()
        thread_repo.conectar()
        svc = tickets(thread_repo)
        r = svc.comprar_numerada(f"t{i}", 555, f"r-conc-{i}")
        with lock:
            resultados.append(r.ok)
        thread_repo.desconectar()

    hilos = [threading.Thread(target=comprar, args=(i,)) for i in range(5)]
    for t in hilos:
        t.start()
    for t in hilos:
        t.join()

    assert sum(resultados) == 1
    assert resultados.count(False) == 4
    print("  [OK] Concurrencia: 5 hilos -> 1 ganador, 4 fallos")


if __name__ == "__main__":
    print("=" * 50)
    print("TEST BASE: PostgreSQL + Tickets")
    print("=" * 50)
    test_numerada()
    test_numerada_idempotencia()
    test_unnumbered()
    test_unnumbered_sold_out()
    test_concurrencia()
    print("\nTODOS LOS TESTS PASARON")
