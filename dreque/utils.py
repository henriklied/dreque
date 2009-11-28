try:
     import procname
     setprocname = procname.setprocname
     getprocname = procname.getprocname
except ImportError:
    try:
        from ctypes import cdll, byref, create_string_buffer
        libc = cdll.LoadLibrary('libc.so.6')
        def setprocname(name):
            buff = create_string_buffer(len(name)+1)
            buff.value = name
            libc.prctl(15, byref(buff), 0, 0, 0)
            # FreeBSD: libc.setproctitle(name)
        def getprocname():
            libc = cdll.LoadLibrary('libc.so.6')
            buff = create_string_buffer(128)
            # 16 == PR_GET_NAME from <linux/prctl.h>
            libc.prctl(16, byref(buff), 0, 0, 0)
            return buff.value
    except (OSError, ImportError):
        def setprocname(name):
            pass
        def getprocname():
            import sys
            return sys.argv[0]
