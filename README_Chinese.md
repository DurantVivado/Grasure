[![Go Reference](https://pkg.go.dev/badge/github.com/DurantVivado/Grasure.svg)](https://pkg.go.dev/github.com/DurantVivado/Grasure)
# Grasure

Go 中的通用擦除编码架构
实现最流行的基于擦除的文件系统操作，它很容易使用并集成到其他文件系统中。

项目主页：https://github.com/DurantVivado/Grasure

Godoc：https://pkg.go.dev/github.com/DurantVivado/Grasure


## 项目架构：
<!-- - `main.go` 包含主函数。对于每次运行，在“编码”、“读取”、“更新”、“缩放”、“删除”、...之间进行操作-->

- `erasure-global.go` 包含系统级接口和全局结构和变量

- `erasure-init.go` 包含基本的配置文件（`.hdr.sys`）读写操作，一旦文件发生变化，我们更新配置文件。

- `erasure-errors.go` 包含各种可能错误的定义。

- `erasure-encode.go` 包含条带文件编码的操作，一件很棒的事情是你可以指定数据布局。

- `erasure-layout.go` 您可以指定布局，例如，随机数据分布或一些其他启发式方法。

- `erasure-read.go` 包含条带文件读取操作，如果部分丢失，我们会尝试恢复。

- `erasure-update.go` 包含条带文件更新的操作，如果某些部分丢失，我们会尝试恢复。

- `erasure-recover.go` 处理多磁盘恢复，涉及数据和元数据。

- `erasure-update.go` 包含更新条带文件的操作，如果某些部分丢失，我们会先尝试恢复。

进口：
[reedsolomon 库](https://github.com/klauspost/reedsolomon)


＃＃ 用法
各种 CLI 用法的完整演示位于 `examples/buildAndRun.sh`。你可能会一瞥。
这里我们在目录`./examples`中详细说明以下步骤：

0. 构建项目：
``
go build -o main ./main.go
``

1.在`./examples`中新建一个名为`.hdr.disks.path`的文件，列出你本地磁盘的路径，例如，
``
/home/server1/data/data1
/home/server1/data/data2
/home/server1/data/data3
/home/server1/data/data4
/home/server1/data/data5
/home/server1/data/data6
/home/server1/data/data7
/home/server1/data/data8
/home/server1/data/data9
/home/server1/data/data10
/home/server1/data/data11
/home/server1/data/data12
/home/server1/data/data13
/home/server1/data/data14
/home/server1/data/data15
/home/server1/data/data16
``

2. 初始化系统，你应该明确附加数据（k）和奇偶校验分片（m）的数量以及块大小（以字节为单位），记住k+m不能大于256。
``
./main -md init -k 12 -m 4 -bs 4096 -dn 16
``
`bs` 是以字节为单位的块大小，`dn` 是你打算在 `.hdr.disks.path` 中使用的 diskNum。显然，为了容错目的，您应该保留一些磁盘。

3. 编码一个示例文件。
``
./main -md encode -f {源文件路径} -conStripes 100 -o
``

4. 解码（读取）示例文件。
``
./grasure -md read -f {源文件基名} -conStripes 100 -sp {目标文件路径}
``

这里的“conStripes”表示允许同时操作的条带数量，默认值为 100。
`sp` 表示保存路径。

使用`fn`模拟失败的磁盘数量（默认为0），例如`-fn 2`模拟任意两个磁盘的关闭。放心，数据不会真的丢失。

5. 检查哈希字符串以查看编码/解码是否正确。

``
sha256sum {源文件路径}
``
``
sha256sum {目标文件路径}
``

6. 删除存储中的文件（目前不可逆，我们正在努力）：
``
./main -md delete -f {filebasename} -o
``

7. 要更新存储中的文件：
``
./main -md update -f {filebasename} -nf {local newfile path} -o
``

8. 恢复磁盘（例如故障磁盘中的所有文件 blob），并将其传输到备份磁盘。这变成了一项耗时的工作。
之前的磁盘路径文件将重命名为`.hdr.disks.path.old`。新的磁盘配置路径将用冗余路径替换每个失败的路径。
``
./main -md 恢复
``


## 存储系统结构
我们使用 `tree` 命令显示存储系统的结构。如下图所示，每个`file`都被编码并分成`k`+`m`个部分，然后保存在`N`个磁盘中。每个名为“BLOB”的部分都放置在一个具有相同基本名称“file”的文件夹中。并且系统的元数据（例如，文件名、文件大小、文件哈希和文件分布）记录在 META 中。关于可靠性，我们复制了 `META` 文件 K-fold。（K 是大写的，不等于前面提到的 `k`）。它用作一般纠删码实验设置，并且很容易集成到其他系统中。
它目前支持 `encode`、`read`、`update` 和更多即将推出的功能。
 ``
 server1@ubuntu:~/data$ tree . -Rh
.
├── [4.0K] 数据1
│ ├── [4.0K] Goprogramming.pdf
│ │ └── [1.3M] BLOB
│ └── [ 46K] META
├── [4.0K] 数据10
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data11
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data12
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data13
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data14
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data15
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data16
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data17
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data18
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data19
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data2
│ ├── [4.0K] Goprogramming.pdf
│ │ └── [1.4M] BLOB
│ └── [ 46K] META
├── [4.0K] data20
│ └── [4.0K] Goprogramming.pdf
│ └── [1.5M] BLOB
├── [4.0K] data21
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
├── [4.0K] data22
│ └── [4.0K] Goprogramming.pdf
│ └── [1.3M] BLOB
├── [4.0K] data23
│ └── [4.0K] Goprogramming.pdf
│ └── [1.4M] BLOB
``


## CLI 参数
`./examples/main.go` 的命令行参数如下所示。
|参数（别名）|说明|默认|
|--|--|--|
|blockSize(bs)|以字节为单位的块大小|4096|
|mode(md)|ec系统的模式，(编码、解码、更新、缩放、恢复)之一||
|dataNum(k)|数据分片的数量|12|
|parityNum(m)|奇偶校验分片的数量（容错）|4|
|diskNum(dn)|磁盘数量（可能比`.hdr.disk.path`中列出的要少）|4|
|filePath(f)|upload：本地文件路径，download&update：远程文件basename||
|savePath|本地保存路径（local path）|file.save|
|newDataNum(new_k)|新的数据分片数|32|
|newParityNum(new_m)|新的奇偶校验分片数|8|
|recoveredDiskPath(rDP)|恢复磁盘的数据路径，默认为/tmp/restore| /tmp/恢复|
|override(o)|是否覆盖之前的文件或目录，默认为false|false|
|conWrites(cw)|是否开启并发写入，默认为false|false|
|conReads(cr)|是否开启并发读取，默认为false|false|
|failMode(fmd)|模拟 [diskFail] 或 [bitRot] 模式"|diskFail|
|failNum(fn)|模拟多盘故障，提供故障盘数|0|
|conStripes(cs)|允许同时编码/解码的条带数量|100|
|quiet(q)|终端输出是否静音|false|

## 表现
性能在测试文件中进行测试。

## 贡献
项目遇到问题时请 fork 和 issue。

它也适用于发送至 [durantthorvals@gmail.com]() 的电子邮件。