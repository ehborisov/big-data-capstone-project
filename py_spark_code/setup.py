from setuptools import setup

setup_config = {
    'description': 'Module with spark-related code for capstone tasks.',
    'author': 'Borisov Egor',
    'version': '1.0',
    'packages': ['spark_code'],
    'install_requires': [
        'pyspark=2.4.0'
    ],
    'name': 'py_spark_code'
}


def main():
    setup(**setup_config)


if __name__ == '__main__':
    main()
