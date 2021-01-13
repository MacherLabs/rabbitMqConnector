from setuptools import setup

setup( 
    name='rabbitMqConnector',
    
    version='0.1',
    description='Face demographics based on age and gender',
    url='http://demo.vedalabs.in/',

    # Author details    
    author='Kumar',
    author_email='tech@gmail.com',

    packages=['rabbitMqConnector'],
    install_requires=['pika==1.1.0','rabbitmq-admin','requests','anytree'] ,

    zip_safe=False
    )