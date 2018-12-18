package com.okooo.betbrain.transit.client;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class TransitConnection {

	private Logger logger = Logger.getLogger(TransitConnection.class);

	private long reConnect = 10 * 60 * 1000;
	private ChannelPipelineFactory channelPipelineFactory;
	private String hostName;
	private int port;
	private ChannelFactory factory;
	private ClientBootstrap bootstrap = null;
	private Timer timer = null;
	private ChannelFuture future;
	private Channel ch = null;
	private ShareBean shareBean = ShareBean.getInstance();
	private long crontabCheckConnectTime;
	private long unreceivedMsgWaitTime;
	private long disconnectedWaitTime;

	public void doStart() {

		factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool(new ThreadFactory() {

					public Thread newThread(Runnable runnable) {
						Thread thread = new Thread(Globals.getThreadGroup(),
								runnable);
						return thread;
					}
				}));
		bootstrap = new ClientBootstrap(factory);
		bootstrap.setPipelineFactory(channelPipelineFactory);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		logger.info("开始连接消息服务器：" + hostName + ":" + port);
		future = bootstrap.connect(new InetSocketAddress(hostName, port));
		ch = future.awaitUninterruptibly().getChannel();
		timer = new Timer();
		shareBean.setConnected(false);
		if (future.isSuccess()) {
			shareBean.setConnected(true);
			// 定时任务：检测连接是否可用
			timer.schedule(new TimerTask() {
				public void run() {
					if (logger.isDebugEnabled()) {
						logger.debug("执行连接检测");
					}

					if (ch != null && shareBean.isConnected()) {

						long time = System.currentTimeMillis()
								- shareBean.getLastConnectedTime();

						if (time / 1000 > unreceivedMsgWaitTime) {
							logger.error("已" + unreceivedMsgWaitTime + "S未接收到服务器的消息,服务器可能已断开连接,重新执行连接...");
							shareBean.setConnected(false);
							doRestart();
						}
					} else if (!shareBean.isConnected()) {
						Date date = new Date();
						long time = date.getTime()
								- shareBean.getLastConnectedTime();
						if (time / 1000 > disconnectedWaitTime) {
							logger.error("已" + disconnectedWaitTime + "S未连接到服务器,重新执行连接...");
							doRestart();
						}
					}
				}

			},1 * 60 * 1000, crontabCheckConnectTime * 1000);
			logger.info("消息系统客户端启动成功");
		} else {
			logger.error("连接出现错误：", future.getCause());
			doRestart();
		}
	}

	public void doStop() {
		logger.info("关闭与服务器连接....");
		final Timer oldtimer = timer;
		final ClientBootstrap oldbootstrap = bootstrap;
		final ChannelFactory oldfactory = factory;
		final Channel oldch = ch;
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				if (oldtimer != null) {
					oldtimer.cancel();
					oldtimer.purge();
				}

				try {
					oldch.disconnect();
				} catch (Exception e) {
				}

				try {
					oldfactory.releaseExternalResources();
				} catch (Exception e) {
					logger.error(e);
				}

				try {
					oldbootstrap.releaseExternalResources();
				} catch (Exception e) {
					logger.error(e);
				}
			}

		});
		t.start();
		timer = null;
		factory = null;
		bootstrap = null;
		logger.info("服务停止成功");
	}

	public void doRestart() {

		logger.info("重新启动betbrain订阅服务.....");
		try {
			doStop();
		} catch (Exception e) {
			logger.error("停止服务发生错误：", e);
		}

		Runtime.getRuntime().gc();
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
		}
		logger.info("重新连接betbrain服务器");
		doStart();
	}

	@Inject
	public void setChannelPipelineFactory(
			ChannelPipelineFactory channelPipelineFactory) {
		this.channelPipelineFactory = channelPipelineFactory;
	}

	@Inject
	public void setHostName(@Named("server.host") String hostName) {
		this.hostName = hostName;
	}

	@Inject
	public void setPort(@Named("server.port") int port) {
		this.port = port;
	}

	@Inject
	public void setUnreceivedMsgWaitTime(@Named("unreceivedMsgWaitTime") int unreceivedMsgWaitTime) {
		this.unreceivedMsgWaitTime = unreceivedMsgWaitTime;
	}
	
	@Inject
	public void setDisconnectedWaitTime(@Named("disconnectedWaitTime") int disconnectedWaitTime) {
		this.disconnectedWaitTime = disconnectedWaitTime;
	}
	
	@Inject
	public void setCrontabCheckConnectTime(@Named("crontabCheckConnectTime") int crontabCheckConnectTime) {
		this.crontabCheckConnectTime = crontabCheckConnectTime;
	}
}
