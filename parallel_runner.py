"""
Created : Mon Oct 22 11:52:37 GMT 2018
Author : David Orn, david@iiim.is

Objective
Wrapper for running a system in parallel, i.e. multiple processes with multiple GPUs
"""

import multiprocessing as mt
import torch


# Setup the general reference


def parallel(function, THREADS, *args):
    """
    Run a parallel function, the inputs of the functions are the GPU identifiacation numbers.
    The range value needs to be tested with to maximize the usage of the GPU

    *args*
        function : python function objectl
    """
    # Unpack args
    args = args[0]

    # Create an empty list to maintain pointers to the procees objects
    processes = []

    # Setup a dynamic source for utilizing all available cuda enabled GPU's
    N_CUDA = int(torch.cuda.device_count())
    CUDA_CYCLE = 0
    for K in range(THREADS):
        # Rotate the access to the GPUs, if there are more then one available
        if N_CUDA > 1:
            id = CUDA_CYCLE
            CUDA_CYCLE += 1
            if CUDA_CYCLE == N_CUDA:
                CUDA_CYCLE = 0
        else:
            id = 0



        print("Initializing CUDA with {}".format(id))

        # Create a process, linked to each GPU
        P = mt.Process(target=function, args=(id, args[K]))
        # Start the process
        P.start()
        # Save the object
        processes.append(P)

        # Join each thread
    for proc in processes:
        proc.join()

    print("Good Exit")
