# Build Hadoop LZO jar on Mac

Please follow steps below  to build:

1. install `lzo-2.10` using `brew`:
```bash
brew install lzo
```
2. download `hadoop-lzo` source code using `Git`:

```bash
git clone git@github.com:twitter/hadoop-lzo.git
```

3. cd into hadoop-lzo directory and type commonds below:

```bash
export CFLAGS=-m64
export CXXFLAGS=-m64
export C_INCLUDE_PATH=/usr/local/Cellar/lzo/2.10/include
export LIBRARY_PATH=/usr/local/Cellar/lzo/2.10/lib
mvn clean package -Dmaven.test.skip=true
```

4. Then got jars under the `tartget` directory and add them to the gradle, DONE.