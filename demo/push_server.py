from bottle import get, post, request, run, response# or route
from time import time as now
from hashlib import md5, sha512

apikey = "#!@$%^dlf$%@!*"

@post('/mw/matches')
def do_matches():
    response.content_type = "json"

    ts = request.query.ts
    at = request.query.at

    if not (ts and at and ts.isdigit())  :
        response.status = 400
        print "bad_argument"
        return {"msg": "bad_argument"}

    if at != md5(sha512("{}{}".format(apikey, ts)).hexdigest()).hexdigest():
        response.status = 401
        print "auth_bad_access_token"
        return {"msg": "auth_bad_access_token"}

    if int(now()) > int(ts):
        response.status = 401
        print "auth_expired_access_token"
        return {"msg": "auth_expired_access_token"}

    req_data = request.body.read()
    print req_data
    return {"msg":"ok"}

run(host='localhost', port=8088)

