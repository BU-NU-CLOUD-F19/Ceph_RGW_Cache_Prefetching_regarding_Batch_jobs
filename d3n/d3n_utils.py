#!/usr/bin/python
import subprocess
import d3n_cfg as cfg


def get_token():
    while(1):
        _token = subprocess.Popen(["swift -A http://%s:%d/auth/1.0 -U %s:swift -K %s auth | grep AUTH_rgwtk | awk -F = '{print $2'}" %(cfg.d3n_host, cfg.d3n_port, cfg.d3n_usr, cfg.d3n_key)], shell=True, stdout=subprocess.PIPE).communicate()[0]
        if _token != "":
            return str(_token[:-1].decode("utf-8"))
        time.sleep(5)


def prefetch_object(token, bucket_name, obj_name, s_off, e_off):
    print(bucket_name, obj_name)
    local_p = subprocess.Popen(["curl -i http://%s:%d/swift/v1/%s/%s -X GET -H \"range: bytes=%d-%d\" -H \"KARIZ_PREFETCH: 1\" -H \"X-Auth-Token: %s\" -o /dev/null 2>&1 | tee %soutput-%s.txt" %(cfg.d3n_host, cfg.d3n_port, bucket_name, obj_name, s_off, e_off, token, './', obj_name)], shell=True)
    local_p.wait()

def evict_object(token, bucket_name, obj_name):
    local_p = subprocess.Popen(["curl -i http://%s:%d/swift/v1/%s/%s -X DELETE -H \"KARIZ_EVICT: 1\" -H \"X-Auth-Token: %s\" -o /dev/null 2>&1 | tee %soutput-%s.txt" %(cfg.d3n_host, cfg.d3n_port, bucket_name, obj_name, token, './', obj_name)], shell=True)
    local_p.wait()


'''
bucket_name = 'test1'

token = get_token()
print(token)

prefetch_object(token, bucket_name, obj_name, 0, 1073741824)

obj_del = 'char2'
s_off = 0
e_off = 0
evict_object(token, bucket_name, obj_del)
#prefetch_object(token, bucket_name, obj_name, s_off, e_off)
'''
