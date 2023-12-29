from distutils.core import setup

setup(
    name='vivian_airflow_operators',
    version='0.2',
    packages=[
        'vivian_airflow_operators',
        'vivian_airflow_operators.hooks',
        'vivian_airflow_operators.operators',
        'vivian_airflow_operators.sensors',
    ],
    install_requires=['apache-airflow']
)
