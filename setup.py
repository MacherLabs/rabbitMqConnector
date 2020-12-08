from setuptools import setup

setup( 
    name='rabbitMqConnector',
    
    version='0.01',
    description='Face demographics based on age and gender',
    url='http://demo.vedalabs.in/',

    # Author details    
    author='Kumar',
    author_email='tech@gmail.com',

    packages=['rabbitMqConnector'],
    install_requires=['pika','requests','anytree','python-dotenv'] ,

    zip_safe=False
    )