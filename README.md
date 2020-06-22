## 1 环境搭建(CentOS7)
### 1.1 安装python和Git
请自行安装python3.6和Git。
使用Git拉取代码： 
```
git clone https://github.com/i-harbor/iharbor-s3.git
```
### 1.2 安装python虚拟环境和包管理工具pipenv
使用pip命令安装pipenv。  
```pip3 install pipenv```
### 1.3  使用pipenv搭建python虚拟环境
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```pipenv install```
### 1.4 数据库安装
请自行安装mysql数据库。
目录s3server下添加security.py文件，security.py中定义了一些安全敏感信息，内容参考security_demo.py；
security.py文件在settings.py文件最后导入了，根据自己的情况修改文件中有关数据库的配置。

### 1.5 ceph配置和依赖库安装
与ceph的通信默认使用官方librados的python包python36-rados，python36-rados的rpm包安装成功后，python包会自动安装到
系统python3第三方扩展包路径下（/usr/lib64/python3.6/site-packages/），然后需要把路径下的python包文
件rados-2.0.0-py3.6.egg-info和rados.cpython-36m-x86_64-linux-gnu.so复制到你的虚拟python环境*/site-packages/下。
```
wget http://download.ceph.com/rpm-nautilus/el7/x86_64/librados2-14.2.1-0.el7.x86_64.rpm
wget http://download.ceph.com/rpm-nautilus/el7/x86_64/python36-rados-14.2.1-0.el7.x86_64.rpm
yum localinstall -y librados2-14.2.1-0.el7.x86_64.rpm python36-rados-14.2.1-0.el7.x86_64.rpm
```
ceph的配置： 
相关配置文件请放到/etc/ceph路径下；  
```
CEPH_RADOS = {
    'CLUSTER_NAME': 'ceph',
    'USER_NAME': 'client.objstore',
    'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
    'KEYRING_FILE_PATH': '/etc/ceph/ceph.client.admin.keyring',
    'POOL_NAME': ('poolname1', 'poolname1'),
}
```

## 2 运行
激活python虚拟环境  
```pipenv shell```

运行web服务
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```python manage.py runserver {HOST}:{PORT}```   



