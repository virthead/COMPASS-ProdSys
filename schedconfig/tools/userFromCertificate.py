import re

def _dictify_dn(dn):
    ret = dict()
    zip_list = [x.split('=') for x in dn.split(',') if '=' in x]

    for attr, value in zip_list:
        if attr in ret.keys():
            ret[attr].append(value)
        else:
            ret[attr] = [value,]
    
    return ret

def user_dict_from_dn(dn):
    d = _dictify_dn(dn)
    ret = dict()

    first_name, last_name = filter(lambda x: ' ' in x, d['CN'])[-1].split(' ')
    ret['last_name'] = last_name
    ret['first_name'] = first_name

    # Letters, digits and @/./+/-/_ only.
    username = dn.replace('=', '-').replace(' ', '_').replace(',', '__')
    username = re.sub(r'[^\w\+\.@-]', '', username) # _ is part of /w

    ret['username'] = username

    ret['password'] = ''  # required fields
    if 'emailAddress' in d.keys():
        ret['email'] = d['emailAddress'][0]
    
    return ret
