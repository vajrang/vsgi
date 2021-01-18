def chunk(lst, num_chunks):
    """Chunks a given list into number of chunks
    
    >>> chunk([0, 1, 2, 3, 4, 5, 6, 7], 3)
    [[0, 3, 6], [1, 4, 7], [2, 5]]
    """
    retval = [lst[i::num_chunks] for i in range(num_chunks)]
    return retval
