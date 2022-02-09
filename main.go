package main

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func main() {
	// 通过 clientv3.Config 配置，客户端参数
	cli, err := clientv3.New(clientv3.Config{
		// etcd 服务端地址数组，可以配置一个或者多个
		Endpoints: []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		// 连接超时时间，5秒
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	// 用完后关闭客户端
	defer cli.Close()

	// 1.Put
	{
		// 获取上下文，设置请求超时时间为5秒
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		// 设置 key="/tizi365/url" 的值为 www.tizi365.com
		_, err = cli.Put(ctx, "/tizi365/url", "www.tizi365.com")

		if err != nil {
			log.Fatal(err)
		}
	}

	// 2.Get
	{
		// 获取上下文，设置请求超时时间为5秒
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		// 读取 key="/tizi365/url" 的值
		resp, err := cli.Get(ctx, "/tizi365/url")
		if err != nil {
			log.Fatal(err)
		}

		// 虽然这个例子我们只是查询一个 Key 的值，
		// 但是 Get 的查询结果可以表示多个 Key 的结果,例如我们根据 Key 进行"前缀匹配",Get 函数可能会返回多个值。
		for _, ev := range resp.Kvs {
			log.Printf("%s : %s\n", ev.Key, ev.Value)
		}
	}
	// 2.1 Get --prefix
	{
		// 获取上下文，设置请求超时时间为5秒
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		// 读取 key 前缀等于"/tizi365/"的所有值
		// 加上 clientv3.WithPrefix() 参数代表 key 前缀匹配的意思
		resp, err := cli.Get(ctx, "/tizi365/", clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}

		// 遍历查询结果
		for _, ev := range resp.Kvs {
			log.Printf("%s : %s\n", ev.Key, ev.Value)
		}
	}

	// 3.Delete
	{
		// 获取上下文，设置请求超时时间为5秒
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		// 删除 key="/tizi365/url" 的值
		_, err = cli.Delete(ctx, "/tizi365/url")

		if err != nil {
			log.Fatal(err)
		}
	}

	//3.1 Delete --prefix
	{
		// 获取上下文，设置请求超时时间为5秒
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

		// 批量删除 key 以"/tizi365/"为前缀的值
		// 加上 clientv3.WithPrefix() 参数代表 key 前缀匹配的意思
		_, err = cli.Delete(ctx, "/tizi365/", clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}
	}

	// 4.Watch
	{
		// 监控 key=/tizi 的值
		watchChan := cli.Watch(context.Background(), "/tizi", clientv3.WithPrevKV())

		// 通过 channel 遍历 key 的值的变化
		for watchResponse := range watchChan {
			for _, ev := range watchResponse.Events {
				log.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}

	// 4.1 Watch --prefix
	{
		// 监控以 /tizi 为前缀的所有 key 的值
		watchChan := cli.Watch(context.Background(), "/tizi", clientv3.WithPrefix())

		// 通过 channel 遍历 key 的值的变化
		for watchResponse := range watchChan {
			for _, ev := range watchResponse.Events {
				log.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}

	// 5.Lease
	{
		// （1）申请一个 10 秒的租约
		leaseResp, err := cli.Grant(context.Background(), 10)
		if err != nil {
			log.Fatal(err)
		}

		//拿到租约的id
		leaseID := leaseResp.ID

		// （2）设置续租（续租就相当于是“心跳”）
		ctx, cancelFunc := context.WithCancel(context.Background())
		keepAliveChan, err := cli.KeepAlive(ctx, leaseID)
		if err != nil {
			log.Fatal(err)
		}

		// （3）监听续租
		go func() {
			for {
				select {
				case leaseKeepResp := <-keepAliveChan:
					if leaseKeepResp == nil {
						log.Printf("已经关闭续租功能\n")
						return
					} else {
						log.Printf("续租成功\n")
						goto END
					}
				}
			END:
				time.Sleep(500 * time.Millisecond)
			}
		}()

		// (4)监听某个 key 的变化
		go func() {
			watchChan := cli.Watch(context.Background(), "/job/v3/1")
			for wr := range watchChan {
				for _, e := range wr.Events {
					log.Printf("type:%v kv:%v  prevKey:%v \n ", e.Type, string(e.Kv.Key), e.PrevKv)
				}
			}
		}()

		//(5) put 一个 kv，让它与租约关联起来，从而实现 10 秒后自动过期
		putResp, err := cli.Put(context.TODO(), "/job/v3/1", "koock", clientv3.WithLease(leaseID))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%v\n", putResp.Header)

		// （6）关闭续租功能
		cancelFunc()

		time.Sleep(2 * time.Second)

		// (7) 撤销租约(撤销租约会"删除"与该租约绑定的所有 k)
		_, err = cli.Revoke(context.TODO(), leaseID)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("撤销租约成功")

		getResp, err := cli.Get(context.TODO(), "/job/v3/1")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%v", getResp.Kvs)
		time.Sleep(20 * time.Second)
	}
}
