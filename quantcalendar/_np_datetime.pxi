# from libc.stdint cimport INT64_MAX
from numpy cimport (
    int64_t,
    npy_datetime,
    NPY_DATETIMEUNIT,
    is_timedelta64_object,
    is_datetime64_object,
    get_datetime64_value,
    get_timedelta64_value,
    get_datetime64_unit,
    PyTimedeltaScalarObject,
)

from ._quantcalendar cimport (
    tp,
    fromtimestamp,
    fromtimestamp_milli,
    fromtimestamp_micro,
    fromtimestamp_nano,
    Datetime,
    microseconds
)

from cpython.datetime cimport (
    PyDateTime_Check,
    PyDateTime_DATE_GET_HOUR,
    PyDateTime_DATE_GET_MICROSECOND,
    PyDateTime_DATE_GET_MINUTE,
    PyDateTime_DATE_GET_SECOND,
    PyDateTime_GET_DAY,
    PyDateTime_GET_MONTH,
    PyDateTime_GET_YEAR,
    import_datetime,
)

import_datetime()

from numpy import datetime64


cdef inline NPY_DATETIMEUNIT get_timedelta64_unit(object obj) noexcept nogil:
    """
    returns the unit part of the dtype for a numpy timedelta64 object.
    """
    return <NPY_DATETIMEUNIT>(<PyTimedeltaScalarObject*>obj).obmeta.base

cdef int64_t get_conversion_factor(NPY_DATETIMEUNIT from_unit, NPY_DATETIMEUNIT to_unit):
    """
    Find the factor by which we need to multiply to convert from from_unit to to_unit.
    """
    cdef int64_t value, overflow_limit, factor
    if (
        from_unit == NPY_DATETIMEUNIT.NPY_FR_GENERIC
        or to_unit == NPY_DATETIMEUNIT.NPY_FR_GENERIC
    ):
        raise ValueError("unit-less resolutions are not supported")
    if from_unit > to_unit:
        raise ValueError("from_unit must be <= to_unit")

    if from_unit == to_unit:
        return 1

    if from_unit == NPY_DATETIMEUNIT.NPY_FR_W:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_D, to_unit)
        factor = 7
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_D:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_h, to_unit)
        factor = 24
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_h:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_m, to_unit)
        factor = 60
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_m:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_s, to_unit)
        factor = 60
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_s:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_ms, to_unit)
        factor = 1000
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_ms:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_us, to_unit)
        factor = 1000
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_us:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_ns, to_unit)
        factor = 1000
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_ns:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_ps, to_unit)
        factor = 1000
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_ps:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_fs, to_unit)
        factor = 1000
    elif from_unit == NPY_DATETIMEUNIT.NPY_FR_fs:
        value = get_conversion_factor(NPY_DATETIMEUNIT.NPY_FR_as, to_unit)
        factor = 1000
    else:
        raise ValueError("Converting from M or Y units is not supported.")

    # overflow_limit = INT64_MAX // factor
    # if value > overflow_limit or value < -overflow_limit:
    #     raise OverflowError("result would overflow")

    return factor * value


cdef int64_t convert_reso(
    int64_t value,
    NPY_DATETIMEUNIT from_reso,
    NPY_DATETIMEUNIT to_reso,
    # bint round_ok,
):
    cdef:
        int64_t res_value, mult

    if from_reso == to_reso:
        return value

    elif to_reso < from_reso:
        # e.g. ns -> us, no risk of overflow, but can be lossy rounding
        mult = get_conversion_factor(to_reso, from_reso)
        # div, mod = divmod(value, mult)
        # if mod > 0 and not round_ok:
        #     raise ValueError("Cannot losslessly convert units")

        # # Note that when mod > 0, we follow np.timedelta64 in always
        # #  rounding down.
        # res_value = div
        res_value = value // mult

    elif (
        from_reso == NPY_DATETIMEUNIT.NPY_FR_Y
        or from_reso == NPY_DATETIMEUNIT.NPY_FR_M
        or to_reso == NPY_DATETIMEUNIT.NPY_FR_Y
        or to_reso == NPY_DATETIMEUNIT.NPY_FR_M
    ):
        return NotImplemented

    else:
        # e.g. us -> ns, risk of overflow, but no risk of lossy rounding
        mult = get_conversion_factor(from_reso, to_reso)
        # overflow_limit = INT64_MAX // mult
        # if value > overflow_limit or value < -overflow_limit:
        #     # Note: caller is responsible for re-raising as OutOfBoundsTimedelta
        #     raise OverflowError("result would overflow")

        res_value = value * mult

    return res_value


cdef long long pydatetime_to_unix_timestamp(object dt):
    cdef:
        int year, mon, mday, hour, min, sec, micro
        cdef Datetime[microseconds]* d
    year = PyDateTime_GET_YEAR(dt)
    mon = PyDateTime_GET_MONTH(dt)
    mday = PyDateTime_GET_DAY(dt)
    hour = PyDateTime_DATE_GET_HOUR(dt)
    min = PyDateTime_DATE_GET_MINUTE(dt)
    sec = PyDateTime_DATE_GET_SECOND(dt)
    micro = PyDateTime_DATE_GET_MICROSECOND(dt)
    d = new Datetime[microseconds](year, mon, mday, hour, min, sec, micro)
    try:
        return d.to_timestamp()
    finally:
        del d # delete heap allocated object


cdef npy_datetime as_int64(object dt, NPY_DATETIMEUNIT creso):
    cdef:
        NPY_DATETIMEUNIT self_creso
        npy_datetime value

    if is_datetime64_object(dt):
        self_creso = get_datetime64_unit(dt)
        value = get_datetime64_value(dt)
    elif is_timedelta64_object(dt):
        self_creso = get_timedelta64_unit(dt)
        value = get_timedelta64_value(dt)
    elif PyDateTime_Check(dt):
        self_creso = NPY_DATETIMEUNIT.NPY_FR_us
        value = pydatetime_to_unix_timestamp(dt)
    elif isinstance(dt, int):
        return <npy_datetime>dt
    else:
        raise NotImplementedError("Only [numpy.datetime64, python::datetime, int] is supported.")

    if self_creso == creso:
        return value

    return convert_reso(value, self_creso, creso)


cdef npy_datetime as_int64_d(object dt, NPY_DATETIMEUNIT creso_first, NPY_DATETIMEUNIT creso_second):
    cdef:
        NPY_DATETIMEUNIT self_creso
        npy_datetime value

    if is_datetime64_object(dt):
        self_creso = get_datetime64_unit(dt)
        value = get_datetime64_value(dt)
    elif is_timedelta64_object(dt):
        self_creso = get_timedelta64_unit(dt)
        value = get_timedelta64_value(dt)
    elif PyDateTime_Check(dt):
        self_creso = NPY_DATETIMEUNIT.NPY_FR_us
        value = pydatetime_to_unix_timestamp(dt)
    elif isinstance(dt, int):
        return <npy_datetime>dt
    else:
        raise NotImplementedError("Only [numpy.datetime64, python::datetime, int] is supported.")

    if self_creso == creso_first:
        value = convert_reso(value, self_creso, creso_second)
    else:
        value = convert_reso(convert_reso(value, self_creso, creso_first), creso_first, creso_second)
    return value

cdef tp to_timepoint(object dt):
    cdef:
        NPY_DATETIMEUNIT unit
        npy_datetime value

    if is_datetime64_object(dt):
        unit = get_datetime64_unit(dt)
        value = get_datetime64_value(dt)
        if unit == NPY_DATETIMEUNIT.NPY_FR_s:
            return fromtimestamp(value)
        elif unit == NPY_DATETIMEUNIT.NPY_FR_ms:
            return fromtimestamp_milli(value)
        elif unit == NPY_DATETIMEUNIT.NPY_FR_us:
            return fromtimestamp_micro(value)
        elif unit == NPY_DATETIMEUNIT.NPY_FR_ns:
            return fromtimestamp_nano(value)
        else:
            raise NotImplementedError("Only unit 's', 'ms', 'us', 'na' are supported.")
    elif PyDateTime_Check(dt):
        return fromtimestamp_micro(pydatetime_to_unix_timestamp(dt))
        # return fromtimestamp(<double>dt.timestamp()) # local timezone if tzinfo is None
    else:
        raise NotImplementedError("Only [numpy.datetime64, python::datetime] are supported.")

def timedelta_s(dt1, dt2):
    return as_int64(dt1, NPY_DATETIMEUNIT.NPY_FR_s) - as_int64(dt2, NPY_DATETIMEUNIT.NPY_FR_s)

def timedelta_ms(dt1, dt2):
    return as_int64(dt1, NPY_DATETIMEUNIT.NPY_FR_ms) - as_int64(dt2, NPY_DATETIMEUNIT.NPY_FR_ms)

def timedelta_us(dt1, dt2):
    return as_int64(dt1, NPY_DATETIMEUNIT.NPY_FR_us) - as_int64(dt2, NPY_DATETIMEUNIT.NPY_FR_us)

def timedelta_ns(dt1, dt2):
    return as_int64(dt1, NPY_DATETIMEUNIT.NPY_FR_ns) - as_int64(dt2, NPY_DATETIMEUNIT.NPY_FR_ns)

cpdef inline npy_datetime to_daily(object dt):
    return as_int64_d(dt, NPY_DATETIMEUNIT.NPY_FR_D, NPY_DATETIMEUNIT.NPY_FR_s)

def timestamp_s(dt):
    return as_int64(dt, NPY_DATETIMEUNIT.NPY_FR_s)

def timestamp_ms(dt):
    return as_int64(dt, NPY_DATETIMEUNIT.NPY_FR_ms)

def timestamp_us(dt):
    return as_int64(dt, NPY_DATETIMEUNIT.NPY_FR_us)

def timestamp_ns(dt):
    return as_int64(dt, NPY_DATETIMEUNIT.NPY_FR_ns)

def combine(date, time):
    return datetime64(as_int64(date, NPY_DATETIMEUNIT.NPY_FR_us) + as_int64(time, NPY_DATETIMEUNIT.NPY_FR_us), "us")
