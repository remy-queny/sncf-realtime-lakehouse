import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path

from run_active_simulation import stop_process, unexpected_exit


def build_real_job_commands(
    project_root,
    reference_root,
    gold_interval=60,
):
    project = Path(project_root).resolve()
    references = Path(reference_root).resolve()

    if type(gold_interval) is not int or gold_interval <= 0:
        raise ValueError("Intervalle Gold entier strictement positif requis.")

    commands = [
        (
            name,
            [
                sys.executable,
                str(project / f"spark/src/jobs/stream_{name}.py"),
            ],
        )
        for name in ("bronze", "silver")
    ]

    commands.append((
        "gold",
        [
            sys.executable,
            str(project / "scripts/run_active_real_gold.py"),
            str(references),
            "--interval",
            str(gold_interval),
        ],
    ))
    return commands


def supervise_real_jobs(
    project_root,
    env,
    commands,
    log_directory,
    stop_requested,
    stop_timeout=60,
    *,
    finite_jobs=(),
):
    project = Path(project_root).resolve()
    logs = Path(log_directory)
    processes = []
    exit_code = 0

    if type(stop_timeout) is not int or stop_timeout <= 0:
        raise ValueError("Délai d'arrêt entier strictement positif requis.")

    finite = set(finite_jobs)
    command_names = {name for name, _ in commands}

    if not finite.issubset(command_names):
        raise ValueError("Un job fini déclaré est absent des commandes.")

    completed = set()
    
    logs.mkdir(parents=True, exist_ok=False)

    with ExitStack() as stack:
        try:
            for name, command in commands:
                if stop_requested.is_set():
                    break

                log = stack.enter_context(
                    (logs / f"{name}.log").open(
                        "x", encoding="utf-8"
                    )
                )

                process = subprocess.Popen(
                    command,
                    cwd=project,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                processes.append((name, process))

                print(
                    f"Processus {name} lancé : PID {process.pid}",
                    flush=True,
                )

            while not stop_requested.wait(0.5):
                monitored = [
                    (name, process)
                    for name, process in processes
                    if name not in completed
                ]

                failure = unexpected_exit(monitored)
                if failure is None:
                    continue

                name, code = failure

                if name in finite and code == 0:
                    completed.add(name)
                    print(
                        f"Processus {name} terminé normalement. "
                        "Les traitements restent actifs.",
                        flush=True,
                    )
                    continue

                raise RuntimeError(
                    f"Sortie inattendue de {name}, code {code}. "
                    f"Consulter {logs / f'{name}.log'}"
                )

        except Exception as error:
            print(
                f"Erreur de supervision réelle : {error}",
                file=sys.stderr,
                flush=True,
            )
            exit_code = 1

        finally:
            for name, process in reversed(processes):
                try:
                    forced = stop_process(
                        name, process, stop_timeout
                    )
                    if forced:
                        exit_code = 1
                except Exception as error:
                    print(
                        f"Erreur pendant l'arrêt de {name} : {error}",
                        file=sys.stderr,
                        flush=True,
                    )
                    exit_code = 1

    return exit_code