from setuptools import setup

setup_config = {
    'description': 'Events generator for Flume netcat source.',
    'author': 'Borisov Egor',
    'version': '1.0',
    'packages': ['events_producer'],
    'install_requires': [
        'random-word=1.0.3'
        'scipy=1.2.1'
    ],
    'name': 'py_events_producer'
}


def main():
    setup(**setup_config)


if __name__ == '__main__':
    main()