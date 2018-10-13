"""
The cron which loads SchedConfig data from CRIC
:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: Oct 2018
"""
### HOW TO RUN
### add base project dir into PYTHONPATH
### $ export PYTHONPATH='/srv/compass/':$PYTHONPATH
### $ python -m schedconfig.crons.schedconf_loader_cron
### schedconfig_url and cache_dir can be additionally customized
### $ python -m schedconfig.crons.schedconf_loader_cron schedconfig_url=http://compass-cric.cern.ch/api/compass/pandaqueue/query/?json tmp_dir=/var/log/compass_prodsys/


import django

import os, sys, urllib2, json, getpass, pytz
from datetime import datetime

from django.conf import settings


class SchedConfigLoaderCron(object):

    def __init__(self, **kwargs):

        from ..models import Schedconfig
        self.Schedconfig = Schedconfig

    def get_tmpdir(self, **kwargs):

        cdir = kwargs.get('tmp_dir')

        if not cdir: # set default tmp_dir
            cdir = os.path.join(os.environ.get('TMP', '/tmp'), 'crons_cache')
            cdir = os.path.join(cdir, os.environ.get('USER', getpass.getuser()))

        if cdir and not os.path.isdir(cdir):
            os.makedirs(cdir)

        return cdir

    @classmethod
    def register_object(self, model, key, key_name, data, defs={}):

        defaults = defs.copy()
        defaults.update(data)

        obj, is_updated = model.objects.get_or_create(**{key_name:key, 'defaults':defaults})
        if not is_updated: ## update object if need
            changes = {}
            for k, v in data.iteritems():
                oldval, newval = getattr(obj, k), v
                is_changed = oldval != newval
                if type(oldval) == datetime and isinstance(newval, basestring):
                    for oval in [oldval.replace(microsecond=0), oldval.replace(microsecond=0, tzinfo=None), oldval.replace(tzinfo=None)]:
                        if unicode(newval) in [oval.isoformat(' '), oval.isoformat()]:
                            is_changed = False

                if is_changed:
                    setattr(obj, k, v)
                    changes[k] = "[%s]'%s'=>[%s]'%s'" % (type(oldval), oldval, type(newval), newval)
            if changes:
                print "*** updating object type=%s, %s=%s, id=%s, mismatches=%s" % (type(obj).__name__, key_name, key, obj.pk, changes)
                obj.save(update_fields=changes.keys())

        return obj, is_updated

    def process_pandaqueues(self, data, **kwargs):

        ## deprecated fields
        ignore_fields = []

        ## default values for fake/deprecated fields which will be used only when object is created 1st time
        ## can be removed once Schedconfig model restrictions are updated
        defvals = {'tspace':datetime.now(), 'space':0, 'auto_mcu':0}

        # {(ext_name, internal name)}
        kmap = {'last_modified':'lastmod'}

        allowed_fields = set([f.name for f in self.Schedconfig._meta.get_fields()]) - set(ignore_fields)
        ret = {}
        for pq, idat in data.iteritems():
            dat = {}
            idat.update(dict((v,idat[k]) for k,v in kmap.iteritems() if k in idat))
            for k in allowed_fields & set(idat):
                if idat[k] is not None:
                    dat[k] = idat[k]

            ret.setdefault(pq, dat)

        ## save cleaned data into cache file for debug and further reference checks
        try:
            cache = os.path.join(self.get_tmpdir(**kwargs), 'schedconfig_cleaned_input.json')
            with open(cache, 'w') as f:
                json.dump(ret, f, indent=2, sort_keys=True)
            print 'Successfully saved cleaned schedconfig data into cache=%s' % cache
        except Exception, e:
            print 'Failed to save cache file=%s .. error=%s .. skipped' % (cache, str(e))

        ## process data
        stats = {}
        for pq, dat in sorted(ret.iteritems()):

            try:
                stats['processed'] = stats.get('processed', 0) + 1
                obj, is_updated = self.register_object(self.Schedconfig, pq, 'nickname', dat, defvals)
                if is_updated:
                    stats['updated'] = stats.get('updated', 0) + 1
            except Exception, e:
                print 'FAILED to process PandaQueue object with name=%s .. skipped, error=%s' % (pq, str(e))
                print 'data=%s' % dat
                stats.setdefault('failed', []).append(pq)
                continue

        return stats


    def run(self, args=[]):


        kwargs = dict((e[0], e[1] if len(e)>1 else '') for r in args for e in [r.split('=', 1)])

        url = kwargs.get('schedconfig_url', 'http://compass-cric.cern.ch/api/compass/pandaqueue/query/?json')
        logdir = getattr(settings, 'LOGGING_DIR', None)

        if logdir:
            kwargs.setdefault('tmp_dir', settings.LOGGING_DIR)

        cache_dir = self.get_tmpdir(**kwargs)
        print '.. loading data from url=%s' % url
        try:
            data = json.load(urllib2.urlopen(url))
        except Exception, e:
            raise
            print 'Failed to load data: %s' % str(e)

        # save to cache
        try:
            cache = os.path.join(cache_dir, 'schedconfig_data.json')
            with open(cache, 'w') as f:
                json.dump(data, f, indent=2, sort_keys=True)
            print 'Successfully saved downloaded schedconfig data into cache=%s' % cache
        except Exception, e:
            print 'Failed to save cache file=%s .. error=%s .. skipped' % (cache, str(e))

        return self.process_pandaqueues(data, **kwargs)


def run(args):

    print '[%s] SchedConfigLoaderCron cron started' % datetime.now()

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compass.settings')
    django.setup()

    ret = SchedConfigLoaderCron().run(args)
    print 'return=%s' % ret
    print '[%s] DONE' % datetime.now()

if __name__ == '__main__':
    run(sys.argv[1:])
