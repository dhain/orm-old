def slice2limit(slc):
    if slc.step is not None:
        raise TypeError('step argument in slice is not supported')
    if slc.stop is None and slc.start is None:
        return
    if (slc.stop is not None and slc.stop < 0) or (slc.start is not None
                                                   and slc.start < 0):
        raise NotImplementedError('negative slice values not yet supported')
    if slc.start is None:
        return 'limit %d' % (slc.stop,)
    limit = 'limit %d' % (slc.start,)
    if slc.stop is None:
        limit += ', -1'
    else:
        limit += ', %d' % (slc.stop - slc.start,)
    return limit
