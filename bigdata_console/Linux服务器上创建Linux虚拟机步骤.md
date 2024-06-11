## 引言

有时候我们需要在同一台计算机中使用多种不同操作系统环境，基于已有的同一堆硬件资源来获得不同操作系统各自的便利性。对此，常用的解决方案主要有：

1. 在物理机器中安装使用双系统
2. 在宿主系统中安装使用虚拟机

第一种在物理机器中安装使用双系统的方式能充分发挥硬件资源的最佳性能，但在切换使用不同操作系统时需要重新启动计算机后再选择切换进入到其他操作系统，相信不只我一个人觉得这样的体验很烦人。

第二种方式首先在物理机器中安装某个操作系统作为宿主系统，然后再在宿主系统中使用虚拟机相关技术和工具虚拟出新的机器以安装和使用其他操作系统环境，虽然在虚拟机中运行的操作系统在性能上比不上第一种方式，在某些场景下（如玩视频游戏以及进行视频渲染等）可能不给力，但这已经足够满足本人的有限需求，而且更为重要的是不需要重启计算机来切换进入不同操作系统，还要啥自行车。因此，个人选择基于这种方式来解决多操作系统的使用问题。

## 虚拟机工具

如果我们只有一台笔记本，又想要搭建一个小集群，怎么办？虚拟机帮你实现梦想，市面上较为常用的虚拟机软件有VMware、VirtualBox、Xen、KVM、hyper-v等。

要在宿主系统中安装使用虚拟机，首先我们需要对虚拟机管理工具做好选型。较为流行的且本人较早接触的虚拟机管理工具主要有[VMWare Workstation Player](https://www.vmware.com/products/workstation-player.html)和[VirtualBox](https://www.virtualbox.org/)，它们都提供了丰富的虚拟机运行管理功能和直观便捷的可视化操作界面，但其对系统资源的占用较高，都只支持在x86架构下运行使用，并且都被商业公司所控制（既使VirtualBox是开源的），前者由VMWare公司提供（2023年11月已被Broadcom收购），后者由Oracle公司提供。是否存在比VMWare Workstation Player和VirtualBox功能更强大，资源占用更友好，以及自由开源的虚拟机管理工具呢？

### QEMU

随着眼界的不断拓展，终于发现了功能更为灵活强大并且不受商业机构控制的开源虚拟机工具[QEMU（Quick Emulator）](https://www.qemu.org/)，准确地说QEMU是模拟器，其除了能提供虚拟机（virtualizer）功能外，更为强大的是能在已有芯片架构的基础上模拟和运行其它架构的系统和应用，即模拟器（Emulator）功能，比如在AMD64/X86_64机器上模拟运行aarch64/ARM64架构的系统和应用。在这里，我们只关注使用其虚拟机功能，类似VMWare Workstation Player和VirtualBox，QEMU属于[Type-2 Hypervisor](https://en.wikipedia.org/wiki/Hypervisor)，提供了基于软件来虚拟化硬件设备资源以创建虚拟机的能力，此外其占用更少的系统资源从而更加高效，而且还支持在非X86架构机器上运行。

### KVM

直接使用QEMU已经可以创建出虚拟机以安装使用隔离于宿主系统的独立操作系统，但其性能受到了较大限制，我们还需要结合使用另一种技术和工具即[KVM（Kernel-based Virtual Machine）](https://en.wikipedia.org/wiki/Kernel-based_Virtual_Machine)。KVM即基于内核的虚拟机技术，属于[Type-1 Hypervisor](https://en.wikipedia.org/wiki/Hypervisor)，能直接基于机器硬件提供虚拟化能力，比如通过优化管理虚拟化CPU与物理CPU间的映射，加速指令在虚拟化CPU的执行。KVM作为内核模块已经被整合到Linux内核中，但需要物理CPU支持硬件虚拟化的扩展能力，如[Intel VT，AMD-V](https://en.wikipedia.org/wiki/Intel_VT)等。QEMU支持与KVM结合使用，以利用其虚拟化加速能力优化虚拟机的运行效率。

### virt-manager+libvirt

QEMU本身只提供了命令行工具去创建、运行和管理虚拟机，这对我们一般用户尤其是习惯使用图形化界面的用户来说不太友好，幸运的是我们可以借助开源可视化虚拟机管理应用[virt-manager](https://virt-manager.org/)来帮助我们通过图形化界面创建、运行和管理虚拟机。

virt-manager主要被用来连接管理QEMU/KVM虚拟机，但也支持管理[Xen](https://en.wikipedia.org/wiki/Xen)和[LXC（Linux容器）](https://en.wikipedia.org/wiki/LXC)，并且同时支持管理本地和远程虚拟机。通过它我们可以创建、配置及监控虚拟机，此外其内置了基于[VNC](https://en.wikipedia.org/wiki/Virtual_Network_Computing)及[SPICE](https://spice-space.org/)协议的窗口查看器以方便我们通过图形化访问使用虚拟机。

virt-manager本身只是一种面向我们终端用户的操作前端，其真正地与QEMU虚拟机管理程序交互还需要利用[libvirt](https://wiki.archlinux.org/title/libvirt)所提供的后台服务。libvirt是一个软件包工具集，提供了一种统一的方式去操作访问多种不同的虚拟化管理程序，包括QEMU、KVM、Xen、LXC、 VMWare ESX以及 VirtualBox等。libvirt提供了一个后台服务[libvirtd](https://www.libvirt.org/manpages/libvirtd.html)，该后台服务负责与具体的虚拟机管理程序交互，virt-manager通过与该后台服务进行通信交互来发起对虚拟机的管理操作。我们除了可以使用virt-manager可视化工具之外，还能使用另一个命令行工具[virsh](https://www.libvirt.org/manpages/virsh.html)与libvirtd交互，不过对于喜欢图形化交互的我们来说，virt-manager无疑是更好的选择。

至此，对所选型的上述虚拟机工具之间的关系进行梳理，可以得到如下所示的交互关系。

```shell
Download
Install it from your OS distribution (others coming soon)

# yum install virt-manager (Fedora)
# apt-get install virt-manager (Debian)
# emerge virt-manager (Gentoo)
# pkg_add virt-manager (OpenBSD
```

### Libvirt 在 OpenStack 中的应用

OpenStack 原生使用 KVM 虚拟化技术, 以 KVM 作为最底层的 Hypervisor, KVM 用于虚拟化 CPU 和内存, 但 KVM 缺少对 *网卡/磁盘/显卡* 等周边 I/O 设备的虚拟化能力, 所以需要 qemu-kvm 的支持, 它构建于 KVM 内核模块之上为 KVM 虚拟机提供完整的硬件设备虚拟化能力.

OpenStack 不会直接控制 qemu-kvm, 而是使用 libvirt 作为与 qemu-kvm 之间的中间件. libvirt 具有跨虚拟化平台能力, 可以控制 VMware/Virtualbox/Xen 等多种虚拟化实现. 所以为了让 OpenStack 具有虚拟化平台异构能录, OpenStack 没有直接调用 qemu-kvm, 而是引入了异构层 libvirt. 除此之外, libvirt 还提供了诸如 pool/vo l管理等高级的功能.

## 虚拟机创建

### 准备工作

1. Windows安装xming工具
2. Linux宿主机上安装相关软件包

`yum install qemu qemu-kvm virt-manager virsh libvirt virt-install -y `

3. Linux 宿主机修改sshd_config配置文件

```shell
vim /etc/ssh/sshd_config 
```

![img](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/20180716193509548.png)

4. 启动libvirtd服务

`systemctl enable --now libvirtd`

4. 重启宿主机`reboot`

5. 打开sshd端的x11数据包转发。任意打开一个终端，然后使用

```
[root@Init ~]# ssh -X IP
#其中IP是你自己服务器的IP,-X的意思是打开X11转发，让GUI能够得以远程实现。
123
```

6. 如果这个时候还报错，那很有可能是你windows上没有x11支持，你需要去下载x11相关软件，并在windws上配置好。
7. 查看支持的iso版本

`osinfo-query os`

8. 准备iso文件

```shell
mkdir -p /var/lib/libvirt/images
# 将操作系统镜像iso文件上传到该目录中
```

### 命令行创建虚拟机

todo

```shell
# libvirt默认目录
/var/lib/libvirt

[root@localhost vm]# ll /var/lib/libvirt
total 8
drwx--x--x  2 root root    6 Mar 15  2020 boot
drwxr-xr-x  2 root root  140 Sep 28  2023 dnsmasq
drwx--x--x  2 root root    6 Mar 15  2020 filesystems
drwx--x--x  2 root root  280 Sep 28  2023 images
drwx------  2 root root    6 Mar 15  2020 network
drwxr-x--x 28 qemu qemu 4096 Apr 25 14:19 qemu
drwx--x--x  2 root root    6 Mar 15  2020 swtpm
drwx--x--x  2 root root 4096 Nov  7 09:44 vm


# libvirt 虚拟机运行时文件存放目录
mkdir -m 711 /var/lib/libvirt/vm

[root@localhost vm]# ll -rthl
total 519G
-rw------- 1 qemu qemu 199K Nov  7 09:44 data-400g-10.2.73.183.qcow2
-rw------- 1 root root 101G Apr 25 14:19 sp3-x86-4c-16g-100g-10.2.73.template.qcow2
-rw------- 1 qemu qemu 5.3G Apr 25 16:20 sp3-4c-16g-100g-10.2.73.175.qcow2
-rw------- 1 qemu qemu 2.6G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.176.qcow2
-rw------- 1 qemu qemu  15G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.180.qcow2
-rw------- 1 qemu qemu  31G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.177.qcow2
-rw------- 1 qemu qemu  32G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.178.qcow2
-rw------- 1 qemu qemu 5.7G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.181.qcow2
-rw------- 1 qemu qemu  31G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.179.qcow2
-rw------- 1 qemu qemu 5.2G Apr 25 16:21 sp3-4c-16g-100g-10.2.73.174.qcow2
-rw------- 1 qemu qemu 148G Apr 25 16:21 data-200g-10.2.73.182.qcow2
-rw------- 1 qemu qemu  58G Apr 25 16:21 sp3-4c-64g-100g-10.2.73.185.qcow2
-rw------- 1 qemu qemu  78G Apr 25 16:21 sp3-4c-32g-100g-10.2.73.183.qcow2
-rw------- 1 qemu qemu  53G Apr 25 16:21 sp3-8c-32g-100g-10.2.73.182.qcow2
-rw------- 1 qemu qemu  56G Apr 25 16:21 sp3-4c-64g-100g-10.2.73.184.qcow2


# 虚拟机配置文件
mkdir /etc/libvirt/qemu/

[root@localhost qemu]# ll -rhtl
total 144K
drwx------ 3 root root   54 Aug 11  2022  networks
-rw------- 1 root root 3.9K Aug 22  2022  debian11-x86_64.xml
-rw------- 1 root root 6.1K Aug 22  2022  debian11-aarch64.xml
-rw------- 1 root root 4.9K Oct 14  2022 'kylin10.0-0518-bank(SMH2).xml'
-rw------- 1 root root 4.9K Oct 14  2022 'kylin10.0-0518-bank(SMH1).xml'
-rw------- 1 root root 4.3K Nov 15  2022  kylin10.0.xml
-rw------- 1 root root 5.2K Sep 28  2023  kylin10.0-sp3-x86-10.2.73.template.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.174.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.175.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.176.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.177.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.178.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.179.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.180.xml
-rw------- 1 root root 5.2K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.181.xml
-rw------- 1 root root 5.5K Oct  9  2023  kylin10.0-sp3-x86-10.2.73.182.xml
-rw------- 1 root root 5.2K Nov  7 09:25  kylin10.0-sp3-x86-10.2.73.184.xml
-rw------- 1 root root 5.2K Nov  7 09:25  kylin10.0-sp3-x86-10.2.73.185.xml
-rw------- 1 root root 5.5K Nov  7 09:44  kylin10.0-sp3-x86-10.2.73.183.xml
drwxr-xr-x 2 root root 4.0K Nov  7 09:44  autostart

# 虚拟机配置文件修改
/etc/libvirt/qemu/*.xml

```

#### 配置文件

1. 查看配置文件-通过GUI

![image-20240429175815222](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240429175815222.png)

2. 查看配置文件-通过命令行

`/etc/libvirt/qemu/sp3-8c-128g-80g-10.2.74.122.xml`

![image-20240429180101145](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240429180101145.png)

3. 配置文件解析

```xml
<!--
WARNING: THIS IS AN AUTO-GENERATED FILE. CHANGES TO IT ARE LIKELY TO BE
OVERWRITTEN AND LOST. Changes to this xml configuration should be made using:
  virsh edit sp3-8c-128g-80g-10.2.74.122
or other application using the libvirt API.
-->

<domain type='kvm'>
  <name>sp3-8c-128g-80g-10.2.74.122</name>
  <uuid>6861665e-9274-4d27-b67a-42a8460c3538</uuid>
  <metadata>
    <libosinfo:libosinfo xmlns:libosinfo="http://libosinfo.org/xmlns/libvirt/domain/1.0">
      <libosinfo:os id="http://kylin.cn/kylin/10.0"/>
    </libosinfo:libosinfo>
  </metadata>
  <memory unit='KiB'>134217728</memory>
  <currentMemory unit='KiB'>134217728</currentMemory>
  <vcpu placement='static'>8</vcpu>
  <os>
    <type arch='aarch64' machine='virt-4.1'>hvm</type>
    <loader readonly='yes' type='pflash'>/usr/share/edk2/aarch64/QEMU_EFI-pflash.raw</loader>
    <nvram>/var/lib/libvirt/qemu/nvram/sp3-8c-128g-80g-10.2.74.122_VARS.fd</nvram>
    <boot dev='hd'/>
  </os>
  <features>
    <acpi/>
    <gic version='3'/>
  </features>
  <cpu mode='host-passthrough' check='none'/>
  <clock offset='utc'/>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <emulator>/usr/libexec/qemu-kvm</emulator>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='/var/lib/libvirt/images/sp3-8c-128g-80g-10.2.74.122.qcow2'/>
      <target dev='vda' bus='virtio'/>
      <address type='pci' domain='0x0000' bus='0x05' slot='0x00' function='0x0'/>
    </disk>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <target dev='sda' bus='scsi'/>
      <readonly/>
      <address type='drive' controller='0' bus='0' target='0' unit='0'/>
    </disk>
    <controller type='usb' index='0' model='qemu-xhci' ports='15'>
      <address type='pci' domain='0x0000' bus='0x02' slot='0x00' function='0x0'/>
    </controller>
    <controller type='scsi' index='0' model='virtio-scsi'>
      <address type='pci' domain='0x0000' bus='0x03' slot='0x00' function='0x0'/>
    </controller>
    <controller type='pci' index='0' model='pcie-root'/>
    <controller type='virtio-serial' index='0'>
      <address type='pci' domain='0x0000' bus='0x04' slot='0x00' function='0x0'/>
    </controller>
    <controller type='pci' index='1' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='1' port='0x8'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x0' multifunction='on'/>
    </controller>
    <controller type='pci' index='2' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='2' port='0x9'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x1'/>
    </controller>
    <controller type='pci' index='3' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='3' port='0xa'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x2'/>
    </controller>
    <controller type='pci' index='4' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='4' port='0xb'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x3'/>
    </controller>
    <controller type='pci' index='5' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='5' port='0xc'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x4'/>
    </controller>
    <controller type='pci' index='6' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='6' port='0xd'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x5'/>
    </controller>
    <controller type='pci' index='7' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='7' port='0xe'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x6'/>
    </controller>
    <controller type='pci' index='8' model='pcie-root-port'>
      <model name='pcie-root-port'/>
      <target chassis='8' port='0xf'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x7'/>
    </controller>
    <interface type='bridge'>
      <mac address='52:54:00:e8:f4:d8'/>
      <source bridge='br-157'/>
      <model type='virtio'/>
      <address type='pci' domain='0x0000' bus='0x01' slot='0x00' function='0x0'/>
    </interface>
    <serial type='pty'>
      <target type='system-serial' port='0'>
        <model name='pl011'/>
      </target>
    </serial>
    <console type='pty'>
      <target type='serial' port='0'/>
    </console>
    <channel type='unix'>
      <target type='virtio' name='org.qemu.guest_agent.0'/>
      <address type='virtio-serial' controller='0' bus='0' port='1'/>
    </channel>
    <input type='tablet' bus='usb'>
      <address type='usb' bus='0' port='1'/>
    </input>
    <input type='keyboard' bus='usb'>
      <address type='usb' bus='0' port='2'/>
    </input>
    <graphics type='vnc' port='-1' autoport='yes'>
      <listen type='address'/>
    </graphics>
    <video>
      <model type='virtio' heads='1' primary='yes'/>
      <address type='pci' domain='0x0000' bus='0x07' slot='0x00' function='0x0'/>
    </video>
    <memballoon model='virtio'>
      <address type='pci' domain='0x0000' bus='0x06' slot='0x00' function='0x0'/>
    </memballoon>
  </devices>
</domain>
```

todo

### GUI 创建虚拟机

#### 创建步骤

上述准备工作做完的情况，下面开始使用图形化方式创建虚拟机。

大致安装步骤：

1. 选择操作系统镜像ISO文件
2. 分配内存、磁盘大小
3. 设置虚拟机名称（不是主机名），而是在管理工具中的名称
4. 选择网络（此处可以直接选择与宿主机桥接网络）
5. 进入操作系统安装界面，设置管理员账户密码等（每种不同操作系统导向界面都不一样）
6. 配置网络

在virt-manager上登录刚刚创建的虚拟机。

```shell
# ifcfg-enp1s0 网卡名以实际为准
vim /etc/sysconfig/network-scripts/ifcfg-enp1s0
修改IP地址、子网掩码
GATEWAY=10.2.74.126
PREFIX=25

vim /etc/sysconfig/network-scripts/ifcfg-enp2s0（有双网卡的情况）
GATEWAY=10.2.77.254
PREFIX=27

# 网卡配置文件 DEMO1 （Kylin 10）
TYPE=Ethernet
PROXY_METHOD=none
BROWSER_ONLY=no
BOOTPROTO=none
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
#IPV6INIT=yes
#IPV6_AUTOCONF=yes
#IPV6_DEFROUTE=yes
#IPV6_FAILURE_FATAL=no
#IPV6_ADDR_GEN_MODE=stable-privacy
NAME=enp1s0
UUID=1a73fe68-ddce-4837-9692-7bb5d0fbc2b1
DEVICE=enp1s0
ONBOOT=yes
IPADDR=10.2.74.101
PREFIX=25
#IPV6_PRIVACY=no
GATEWAY=10.2.74.126

# 网卡配置文件 DEMO2 （CentOS）
TYPE=Ethernet
PROXY_METHOD=none
BROWSER_ONLY=no
BOOTPROTO=none
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=yes
IPV6_AUTOCONF=yes
IPV6_DEFROUTE=yes
IPV6_FAILURE_FATAL=no
IPV6_ADDR_GEN_MODE=stable-privacy
NAME=eth0
UUID=0380238e-cca0-41a9-83b4-baf1f124f877
DEVICE=eth0
ONBOOT=yes
IPADDR=10.2.74.121
GATEWAY=10.2.74.126
PREFIX=25


# 修改完后
重启对应网卡
ifdown enp1s0
ifup enp2s0

或者

#重启虚机
reboot
```

5. 虚拟机是借助宿主机的网络来进行通信的

假设宿主机的网卡情况是

```shell
[root@host10-2-73-225 ~]# ip a
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
    inet6 ::1/128 scope host 
       valid_lft forever preferred_lft forever
2: enp125s0f0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
3: enp125s0f1: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc fq_codel state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:75 brd ff:ff:ff:ff:ff:ff
4: enp125s0f2: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc fq_codel state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:76 brd ff:ff:ff:ff:ff:ff
5: enp125s0f3: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc fq_codel state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:77 brd ff:ff:ff:ff:ff:ff
6: enp189s0f0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:78 brd ff:ff:ff:ff:ff:ff
7: enp189s0f1: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:79 brd ff:ff:ff:ff:ff:ff
8: enp189s0f2: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:7a brd ff:ff:ff:ff:ff:ff
9: enp189s0f3: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN group default qlen 1000
    link/ether c4:a4:02:49:2b:7b brd ff:ff:ff:ff:ff:ff
11: br-154: <BROADCAST,MULTICAST,PROMISC,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
    inet 10.2.73.161/27 brd 10.2.73.191 scope global noprefixroute br-154
       valid_lft forever preferred_lft forever
    inet6 fe80::d977:42de:47af:9d34/64 scope link noprefixroute 
       valid_lft forever preferred_lft forever
13: enp125s0f0.154@enp125s0f0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master br-154 state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
14: virbr0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc noqueue state DOWN group default qlen 1000
    link/ether 52:54:00:5f:75:cb brd ff:ff:ff:ff:ff:ff
    inet 192.168.124.1/24 brd 192.168.124.255 scope global virbr0
       valid_lft forever preferred_lft forever
15: virbr0-nic: <BROADCAST,MULTICAST> mtu 1500 qdisc fq_codel master virbr0 state DOWN group default qlen 1000
    link/ether 52:54:00:5f:75:cb brd ff:ff:ff:ff:ff:ff
17: vnet0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel master br-154 state UNKNOWN group default qlen 1000
    link/ether fe:54:00:bc:bf:8f brd ff:ff:ff:ff:ff:ff
    inet6 fe80::fc54:ff:febc:bf8f/64 scope link 
       valid_lft forever preferred_lft forever
20: br-156: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
    inet 10.2.73.225/27 brd 10.2.73.255 scope global noprefixroute br-156
       valid_lft forever preferred_lft forever
    inet6 fe80::fda3:aca1:4d29:44b8/64 scope link noprefixroute 
       valid_lft forever preferred_lft forever
22: br-157: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
    inet 10.2.74.4/25 brd 10.2.74.127 scope global noprefixroute br-157
       valid_lft forever preferred_lft forever
    inet6 fe80::fbf0:fa8d:ef53:ad01/64 scope link noprefixroute 
       valid_lft forever preferred_lft forever
24: enp125s0f0.157@enp125s0f0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master br-157 state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
25: enp125s0f0.156@enp125s0f0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master br-156 state UP group default qlen 1000
    link/ether c4:a4:02:49:2b:74 brd ff:ff:ff:ff:ff:ff
```

需要在GUI中将虚拟机的虚拟网络接口，桥接到和虚拟机IP同网段的宿主机的网卡上面，如下图所示：

![image-20240426092456694](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426092456694.png)

5. 使用本地SSH客户端验证，是否能够登录

#### 操作系统导向设置界面

##### CentOS

图形化的操作界面，界面友好、经典

##### 麒麟操作系统

Kylin-Server-V10-SP3-General-Release-2212-X86_64.iso

图形化的操作界面，界面友好、经典。类似于CentOS系统。

#### 设置基线

todo

### 克隆方式创建虚拟机

#### GUI方式

1. 选择要克隆的虚拟机，进行关机

2. 右键->克隆->点击克隆

![image-20240426095546245](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426095546245.png)

3. 等待克隆完毕
4. 克隆完成功之后，账号密码沿用原来的虚拟机
5. 需要修改IP，再reboot

### 命令行方式创建虚拟机

todo

### 设置yum源（repo源）

1. 准备源（注意要和当前操作系统适配）
2. 备份repo配置文件

```shell
cd /etc/yum.repos.d
mkdir repo_bak
mv *.repo repo_bak/
ll
```

3. 配置repo文件

```shell
#DEMO1
###Kylin Linux Advanced Server 10 - os repo###
[ks10-adv-os]
name = Kylin Linux Advanced Server 10 - Os 
baseurl = ftp://10.2.71.1/pub/yum/Kylin-v10-sp3_arm64/ks10-adv-os/
gpgcheck = 0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-kylin
enabled = 1
[ks10-adv-updates]
name = Kylin Linux Advanced Server 10 - ks10-adv-updates
baseurl = ftp://10.2.71.1/pub/yum/Kylin-v10-sp3_arm64/ks10-adv-updates/
gpgcheck = 0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-kylin
enabled = 1

#DEMO2
[rhel-79]
name=Red Hat Enterprise Linux $releasever - $basearch - Source
baseurl=ftp://10.2.71.1/pub/yum/rhel79_x86_64/
enabled=1
gpgcheck=0
[epel7]
name=epel7
baseurl=ftp://10.2.71.1/pub/yum/epel7_x86_64/
enabled=1
gpgcheck=0
```



3. 清空缓存、生成新缓存

```shell
yum clean all
yum makecache
```

5. 安装EPEL源

```shell
yum list | grep epel-release
yum install -y epel-release

再次检查文件，是否生成epel.repo和epel-testing.repo
[root@localhost yum.repos.d]# ll

再次运行yum clean all 清除缓存，运行 yum makecache 生成新的缓存

# 检查仓库是否启用
```





## 虚拟机管理

使用GUI工具或命令行工具，启动虚拟机、停止虚拟机、删除虚拟机、查看虚拟机状态、调整虚拟机配置等。

### Linux命令工具管理

**Linux虚拟机管理命令**

以下命令在宿主机上执行：

```
virt-manager                ##开启图形管理工具
virt-viewer vmname          ##显示虚拟机,vmname表示虚拟机名称
virsh list                  ##列出正在运行的vm
virsh list --all            ##列出所有vm
virsh start vmname          ##运行指定vm
virsh shutdown vmname       ##正常关闭指定vm
virsh destroy vmname        ##强行结束指定vm
virsh create vmname.xml     ##临时恢复指定vm，vmname表示前端管理文件
virsh define vmname.xml     ##永久恢复vm
virsh undefine  vmname      ##删除vm的前端管理，不会删除存储

virsh list --all 查看运行中的虚拟机
virsh shutdown $id   -- 关闭指定虚拟机
关闭所有虚拟机
for i in $(virsh list --name);do virsh shutdown $i;done
查看是否关闭
virsh list --all

# 将当前宿主机上运行的所有的虚拟机设置为开机自启
for i in $(virsh list --name --all);do virsh autostart $i ;done
virsh list --autostart --all    查看开机自启的虚机
```

### Windows GUI管理

1. windows打开xming
2. 宿主机执行`export DISPLAY=10.0.0.1:0.0`（可选）
3. 宿主机上输入：`virt-manager`

![image-20240425131549919](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240425131549919.png)

可以在图形化界面实现以下功能：

- 启动虚拟机
- 暂停虚拟机
- 停止虚拟机
- 重启虚拟机
- 创建虚拟机

#### 对虚拟机的CPU、内存、磁盘进行扩容、缩容

？

#### 虚拟机与宿主机配置时钟同步
TODO

#### 虚拟机时钟同步服务配置

```shell
# 注意：（1）以下命令均需要在root用户下执行，（2）主服务器与其他服务器需要能双向ping通，即网络要通，否则无法同步成功

# 所有服务器安装chrony服务
yum install chrony -y

# 用于其他服务器锚定的主服务器校准时间（源服务器）
sudo date -s "2022-01-01 12:00:00"

# 主服务器（服务端）配置chrony.conf
vi /etc/chrony.conf
在 # Allow NTP Client access from local network. 下面添加
allow all

#  其他服务器（客户端）配置chrony.conf
vi /etc/chrony.conf
将 #Use public servers from the pool.ntp.org project. 下面的替换为
server 主服务器IP ibusrt
将 #Serve time even if not synchronized to any NTP server.注释打开
local stratum 10

# 重启服务端+客户端的chronyd服务（重启之后会自动与服务端同步时间）
service chronyd status
service chronyd restart
service chronyd status

# 让客户端立即时钟同步
chronyc sources -v

# 查看服务端和客户端时间是否一致
date
```




### Q&A

1. Q：输入virt-manager出现报错

![image-20240425140547670](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240425140547670.png)

A：sshd开启TCP Forward，重写reboot宿主机

2. Q: 启动虚拟系统管理器出错：g-io-error-quark: 已到超时限制(24)

![image-20240426090849387](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426090849387.png)

A: 过一会重新试一下

3.  使用XShell连接虚拟机成功，但是过一会就断开，显示”Socket error Event: 32 Error: 10053“

![image-20240426091919208](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426091919208.png)

![image-20240426092349106](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426092349106.png)

4. 安装麒麟操作系统出现

![image-20240426094411422](./Linux%E6%9C%8D%E5%8A%A1%E5%99%A8%E4%B8%8A%E5%88%9B%E5%BB%BALinux%E8%99%9A%E6%8B%9F%E6%9C%BA%E6%AD%A5%E9%AA%A4.assets/image-20240426094411422.png)

ISO镜像选择错误，架构错误





## 参考

- [虚拟机管理器（Virtual Machine Manager）简介 - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/83331819)
- [KVM/QEMU/qemu-kvm/libvirt 概念全解-阿里云开发者社区 (aliyun.com)](https://developer.aliyun.com/article/237277)
- [Ubuntu中安装使用QEMU/KVM/virt-manager运行虚拟机 - beaclnd - 博客园 (cnblogs.com)](https://www.cnblogs.com/beaclnd/p/18047633)
- [Virtual Machine Manager (virt-manager.org)](https://virt-manager.org/)
- [虚拟机管理器（Virtual Machine Manager）简介 - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/83331819)[](https://www.cnblogs.com/JasonCeng/p/14182395.html)
- [KVM图形管理界面打不开（virt-manager 报错）_virt-manager打不开-CSDN博客
- [[使用virt-manager 创建虚拟机_virt manager-CSDN博客](https://blog.csdn.net/Geor_ge_/article/details/109544787)](https://blog.csdn.net/gui951753/article/details/81070445)