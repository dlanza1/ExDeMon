
import ConfigParser
import sys, traceback

def load():
    """
    Reads configuration file
    """
    try:
        # Load configuration from file
        config = ConfigParser.ConfigParser(defaults={'host': '127.0.0.1', 'port': '5000'})
        number_read_files = config.read('/etc/exdemon/api.cfg')

        # check the config file exist and can be read
        if len(number_read_files) != 1:
            print "Configuration file '{0}' cannot be read or does not exist. Stopping.".format(args.config)
            sys.exit(1)
        
        config.set('database', 'connection', 
                ('postgresql://%s:%s@%s:%s/%s' % 
                    (
                        config.get('database', 'user'),
                        config.get('database', 'pass'),
                        config.get('database', 'host'),
                        config.get('database', 'port'),
                        config.get('database', 'database')
                    )
                )
            )
        return config
        
    except IOError as e:
        traceback.print_exc(file=sys.stdout)
        sys.exit(e.code)

# Loads config
config = load()
