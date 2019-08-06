import re

def _dictify_dn(dn):
    ret = {}
    zip_list = [x.split('=') for x in dn.split(',') if '=' in x]

    for attr, value in zip_list:
        if attr in ret.keys():
            ret[attr].append(value)
        else:
            ret[attr] = [value,]
    
    return ret

def user_dict_from_dn(dn):
    d = _dictify_dn(dn)
    ret = {}

    name = filter(lambda x: ' ' in x, d['CN'])[-1].split(' ')
    ret['last_name'] = name[0]
    ret['first_name'] = name[1]

    # Letters, digits and @/./+/-/_ only.
    username = dn.replace('=', '-').replace(' ', '_').replace(',', '__')
    username = re.sub(r'[^\w\+\.@-]', '', username) # _ is part of /w

    ret['username'] = username

    ret['password'] = ''  # required fields
    if 'emailAddress' in d.keys():
        ret['email'] = d['emailAddress'][0]
    
    return ret
