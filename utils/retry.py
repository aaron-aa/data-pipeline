def retry(func, params, retry_count=10):
    """
    Try retry_count times for func if something wrong happened with 1 second interval
    Args:
        func: function you want execute
        params: tuple, params pass to func
    Returns:
        result return by func
    """

    tries = 0

    while tries < retry_count:
        try:
            return func(*params)
        except Exception as e:
            print 'failed to {func}, tried {n} times because of {e}'.format(func=func.__name__, n=(tries + 1), e=e)
            tries += 1
            time.sleep(1)

    if tries >= retry_count:
        raise Exception("failed to retry {func} {n} times".format(func=func.__name__, n=tries))
