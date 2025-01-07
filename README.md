# 交易日历

日历数据来源：

- [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars)
- [Tushare](https://tushare.pro/)
- [tqsdk](https://www.shinnytech.com/)
- [chinese_calendar](https://pypi.org/project/chinesecalendar/)

数据需自行导入数据库。本库测试使用MongoDB。

## 特性

- 支持Python和C++(正在开发中...)两种接口
- 支持不同证券品种生成不同交易日历，比如中国期货
- 支持查询不同周期的K线时间，支持和东方财富期货、新浪期货相同的K线时间

## 安装

### 依赖项

1. 下载并安装 [quantdata](https://github.com/sric0880/quantdata)
2. 下载并安装 [unordered_dense](https://github.com/martinus/unordered_dense)
3. 安装依赖[vcpkg](https://learn.microsoft.com/zh-cn/vcpkg/get_started/get-started-vscode?pivots=shell-powershell)


### C++

#### Windows

在Visual Studio中调试：修改`CMakePresets.json`中的configurePresets > windows-base > CMAKE_PREFIX_PATH。这个是前面安装mongo cxx driver的`CMAKE_INSTALL_PREFIX`，告诉CMake去哪个目录查找MongoDB的库文件。

命令行安装：DCMAKE_PREFIX_PATH 和 DCMAKE_TOOLCHAIN_FILE 都改成自己的目录。MongoDB只是在测试用例中使用，如果不测试，可以不填写`DCMAKE_PREFIX_PATH`，并注释掉CMakeLists.txt中相关测试用例。

```sh
cmake -S . -B ./build -G "Visual Studio 17 2022" -DCMAKE_PREFIX_PATH=C:\"Program Files (x86)"\mongo-c-driver -DCMAKE_TOOLCHAIN_FILE="D:/open_source/vcpkg/scripts/buildsystems/vcpkg.cmake"
cmake --build ./build --config RelWithDebInfo --parallel
cmake --build ./build --target install --config RelWithDebInfo
```

#### MacOS/Linux

```sh
```

### Python

install build tools

```sh
pip install scikit-build
```

#### Windows

同样见C++，需要修改相关参数DCMAKE_PREFIX_PATH 和 DCMAKE_TOOLCHAIN_FILE。使用`pip install`无法传参。

```sh
python .\setup.py install -G "Visual Studio 17 2022" -- -DCMAKE_PREFIX_PATH="C:\Program Files (x86)\mongo-c-driver" -DCMAKE_TOOLCHAIN_FILE="D:\open_source\vcpkg\scripts\buildsystems\vcpkg.cmake"
```

同样也支持原地构建

```sh
python .\setup.py build_ext --inplace -G "Visual Studio 17 2022" -- -DCMAKE_PREFIX_PATH="C:\Program Files (x86)\mongo-c-driver" -DCMAKE_TOOLCHAIN_FILE="D:\open_source\vcpkg\scripts\buildsystems\vcpkg.cmake"
```


#### MacOS/Linux

```sh
pip install .

# build inplace
python setup.py build_ext --inplace

# build wheel
python setup.py bdist_wheel
```


## 测试

使用方法见测试用例

### C++

```sh

```

### Python

先原地构建，再运行

```sh
python -m pytest tests
```
