import ConfigParser as cp

CONFIG_FILE = 'sources.conf'
CONF = cp.ConfigParser()
CONF.readfp(open(CONFIG_FILE))

def get(opt, section='DEFAULT'):
    return CONF.get(section, opt)
