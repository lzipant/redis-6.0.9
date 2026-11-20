# 编译jemalloc
```shell
cd ./deps/jemalloc
autoconf
./configure
make
sudo make install
```

可能最后一步的install_doc会失败，那么可以不安装文档。
```shell
sudo make install_bin install_include install_lib 
```

# 编译Redis
```shell
make distclean # 先清理一下
make
```

# CLion调试redis
在右上角 Run Configuration中，CLion默认会生成一些Native Application，选到redis-server，并为其配置executable，选中src目录下的redis-server可执行文件。