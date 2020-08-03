def sortedmedian(sortedvalues, begin, end):
    n = end - begin
    if n % 2 == 1:
        return sortedvalues[int(begin + (n - 1) / 2)]
    else:
        mid = int(begin + n / 2)
        return (sortedvalues[mid - 1] + sortedvalues[mid]) / 2


def upperbound(arr, begin, end, tar):
    while begin < end:
        mid = int(begin + (end - begin) / 2)
        if arr[mid] <= tar:
            begin = mid + 1
        elif arr[mid] > tar:
            end = mid
    return begin


def medianFilter(data, window, needtwoend):
    wlen = window + 1
    tlen = len(data)
    val = data[:]
    ans = data[:]
    curwindow = data[0:wlen]
    if tlen < wlen:
        return ans
    for i in range(wlen):
        index = i
        add_id = upperbound(curwindow, 0, i, val[i])
        while index > add_id:
            curwindow[index] = curwindow[index - 1]
            index = index - 1
        while len(curwindow) <= add_id + 1:
            curwindow.append(0)
        curwindow[add_id] = data[i]
        if i >= (wlen - 1) / 2 and needtwoend:
            ans[int(i - (wlen - 1) / 2)] = sortedmedian(curwindow, 0, i + 1)
    half_window = window / 2 if window % 2 == 0 else (window - 1) / 2
    half_window = int(half_window)
    ans[int(half_window)] = sortedmedian(curwindow, 0, wlen)
    for i in range(half_window + 1, tlen - half_window):
        deleted_id = upperbound(curwindow, 0, wlen, val[i - half_window - 1]) - 1
        index = deleted_id
        while index < wlen - 1:
            curwindow[index] = curwindow[index + 1]
            index = index + 1
        add_id = upperbound(curwindow, 0, wlen - 1, val[i + half_window])
        index = wlen - 1
        while index > add_id:
            curwindow[index] = curwindow[index - 1]
            index = index - 1
        while len(curwindow) <= add_id + 1:
            curwindow.append(0)
        curwindow[add_id] = data[i + half_window]
        ans[i] = sortedmedian(curwindow, 0, wlen)
    if needtwoend:
        for i in range(tlen - half_window, tlen):
            deleted_id = upperbound(curwindow, 0, wlen, data[i - half_window - 1]) - 1
            index = deleted_id
            while index < wlen - 1:
                curwindow[index] = curwindow[index + 1]
                index = index + 1
            wlen = wlen - 1
            ans[i] = sortedmedian(curwindow, 0, wlen)
    return ans

git
def getboundaryUnits(values, isanomaly):
    # print('values', values)
    # print('isanomaly', isanomaly)
    if len(values) == 0:
        return []
    window = int(min(len(values) / 3, 512))
    trend_fraction = 0.5
    trend_sum = 0
    calculation_size = 0
    # print(values)
    trend = medianFilter(values, window, True)
    # print(trend, 'trend')
    for i in range(len(trend)):
        if not isanomaly[i]:
            trend_sum = trend_sum + abs(trend[i])
            calculation_size = calculation_size + 1
    # print(calculation_size, 'cal')
    trendavg = trend_sum / calculation_size
    units = []
    for i in range(len(trend)):
        units.append(max(1, trendavg * (1 - trend_fraction) + abs(trend[i]) * trend_fraction))
        # print('unit', units[i])
    return units
