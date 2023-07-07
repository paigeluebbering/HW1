import time
import csv
from multiprocessing import Process, Queue


number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1

def calculate_absolute_difference(a, b):
    return abs(a - b)

def worker_logic(rank, recv_f, send_f):
    while True:
        data = recv_f(MPI_ANY_SOURCE)
        if data == 'exit':
            break
        else:
            result = calculate_absolute_difference(data)
            send_f((rank, result), 0)
        
def coordinator_logic(size, send_f, recv_f):
    # Generate inputs for the worker processes
    inputs = [(1, 5), (10, 8), (3, 2), (6, 4)]
    with open('results.csv', 'w', newline='') as csvfile:
        result_writer = csv.writer(csvfile)
        for i in inputs:
            result = calculate_absolute_difference(i[0], i[1])
            result_writer.writerow([result])
    for i in range(1, size):
        send_f('exit', i)

def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)
    
    app_f(process_rank, size, send_f, recv_f)


def _generate_recv_f(process_rank, send_queues):

    def recv_f(from_source:int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f


def _generate_send_f(process_rank, send_queues):

    def send_f(data, dest):
        send_queues[dest].put((process_rank,data))
    return send_f

def _simulate_mpi(n:int, app_f):
    send_queues = {}
    for process_rank in range(n):
        send_queues[process_rank] = Queue()

    ps = []
    for process_rank in range(n):
        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
        
# MPI Application
def mpi_application(rank, size, send_f, recv_f):
    if rank == 0:
        coordinator_logic(size, send_f, recv_f)
    else:
        worker_logic(rank, recv_f, send_f)


# Unit test for the calculate_absolute_difference function
def test_calculate_absolute_difference():
    result = calculate_absolute_difference(5, 10)
    print(f"Actual result: {result}")
    assert result == 5


if __name__ == "__main__":
    test_calculate_absolute_difference()

    # Running MPI simulation
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
