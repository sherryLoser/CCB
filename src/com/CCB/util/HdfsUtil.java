package com.CCB.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.CCB.main.Test;

public class HdfsUtil {
	static Logger logger =Logger.getLogger(HdfsUtil.class);
	private static Configuration conf = new Configuration();
	public static String iNode = null;
	private static Properties hdfs_config = null;		

	static {
		System.setProperty("hadoop.home.dir", "C:/lib/hadoop");
		InputStream ins = HdfsUtil.class.getClassLoader().getResourceAsStream("Hbase.properties");
		hdfs_config = new Properties();
		try {
			hdfs_config.load(ins);
			iNode = hdfs_config.getProperty("hdfsInode");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(FileSystem fileSystem) {
		try {
			if (null != fileSystem) {
				fileSystem.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(FSDataInputStream inputStream){
		try {
			if (null != inputStream) {
				inputStream.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 1 检查文件是否已经存在
	 * 
	 * @return
	 */
	public synchronized static boolean checkFile(String hdfsDst) {
		boolean existed = false;
		try {
			Path f = new Path(hdfsDst);
			FileSystem fs = FileSystem.get(URI.create(iNode), conf);
			existed = fs.exists(f);
			//System.out.println("this file existed:?" + existed);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return existed;
	}

	/**
	 * 2 在HDFS上创建文件
	 */
	public synchronized static void createFile(String path) {
		try {
			Path f = new Path(path);
			System.out.println("Create and Write :" + f.getName() + " to hdfs");
			FileSystem fs = FileSystem.get(URI.create(iNode), conf);
			FSDataOutputStream os = fs.create(f, true);
			Writer out = new OutputStreamWriter(os, "utf-8");// 以UTF-8格式写入文件，不乱码
			out.close();
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 3 写本地文件追加到HDFS的文件上
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized static boolean append(String localSrc, String hdfsDst) throws IOException {
		Configuration conf = new Configuration();
		File file = new File(localSrc);
		boolean succ = false;
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
			FileSystem fs = FileSystem.get(URI.create(iNode), conf);
			OutputStream out = fs.append(new Path(hdfsDst));
			IOUtils.copyBytes(in, out, 4096, true);
			System.out.println("Write " + file.getName() + " to hdfs file " + hdfsDst + " success");
			succ = true;
		} catch (FileNotFoundException e) {
			System.out.println("Write " + file.getName() + " to hdfs file " + hdfsDst + " failed");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return succ;
	}

	/**
	 * 3 写本地文件到HDFS的文件上,直接覆盖
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized static boolean writeToHdfs(String localSrc, String hdfsDst) throws IOException {
		File file = new File(localSrc);
		BufferedReader in;
		boolean succ = false;

		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			// boolean e = fs.exists(new Path(hdfsDst));
			FSDataOutputStream os = fs.create(new Path(hdfsDst), true);
			Writer out = new OutputStreamWriter(os);
			String str = "";
			while ((str = in.readLine()) != null) {
				out.write(str + "\n");
			}
			in.close();
			out.close();
			os.close();
			System.out.println("Write " + file.getName() + " to hdfs file " + hdfsDst + " success");
			succ = true;
		} catch (FileNotFoundException e) {
			System.out.println("Write " + file.getName() + " to hdfs file " + hdfsDst + " failed");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return succ;
	}

	/**
	 * 4 上传文件到HDFS
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized static boolean uploadToHdfs(String localSrc, String hdfsDst)
			throws FileNotFoundException, IOException {
		File file = new File(localSrc);
		String filename = new String(file.getName());
		// String hdfsDst = hdfsDir + "/"+ filename;
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		Path srcPath = new Path(hdfsDst);
		String parentSrc = srcPath.getParent().toString();
		boolean ch = fs.exists(new Path(hdfsDst));
		boolean pa = fs.exists(new Path(parentSrc));
		boolean succ = false;
		if (pa == false) {
			mkdirs(parentSrc);
		}
		if (ch == false) {
			createFile(hdfsDst);
			OutputStream out = fs.create(new Path(hdfsDst), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
			succ = true;
		} else {
			System.out.print("The file has existed on hdfs!");
		}
		return succ;
	}

	/**
	 * 5 删除HDFS文件
	 * 
	 * @return
	 */
	public synchronized static boolean rmFile(String hdfsDst) throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		boolean e = checkFile(hdfsDst);
		if (e == true) {
			e = fs.deleteOnExit(new Path(hdfsDst));
			fs.close();
			//System.out.println(hdfsDst + "has been deleted");
		} else {
			e = false;
			System.out.println("This file is not existed on HDFS");
		}
		return e;
	}

	/**
	 * 6 创建目录和父目录
	 * 
	 * @param fs
	 * @param dirName
	 * @throws IOException
	 */
	public synchronized static boolean mkdirs(String hdfsDir) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		Path workDir = fs.getWorkingDirectory();
		Path src = new Path(hdfsDir);
		boolean ex = fs.exists(src);
		boolean succ = false;
		try {
			if (ex == false) {
				succ = fs.mkdirs(src);
				if (succ) {
					System.out.println("create directory " + hdfsDir + " successed. ");
				} else {
					System.out.println("create directory " + hdfsDir + " failed. ");
				}
			} else {
				System.out.println("The directory has been existed");
			}
		} catch (Exception e) {
			System.out.println("create directory " + hdfsDir + " failed :" + e);
		}

		return succ;
	}

	/**
	 * 7 删除目录和子目录
	 * 
	 * @param dst
	 * @throws IOException
	 */
	public synchronized static boolean rmdirs(String hdfsDir) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		Path workDir = fs.getWorkingDirectory();
		Path src = new Path(hdfsDir);
		boolean succ = false;
		try {
			succ = fs.delete(src, true);
			if (succ) {
				System.out.println("delete directory " + hdfsDir + " successed. ");
			} else {
				System.out.println("delete directory " + hdfsDir + " failed. ");
			}
		} catch (Exception e) {
			System.out.println("delete directory " + hdfsDir + " failed :" + e);
		}
		return succ;
	}

	/**
	 * 8 读取文件
	 */
	public synchronized static boolean readFromHdfs(String hdfsSrc, String localDst)
			throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfsSrc));
		OutputStream out = new FileOutputStream(localDst);
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		boolean succ = false;
		try {
			while (-1 != readLen) {
				out.write(ioBuffer, 0, readLen);
				readLen = hdfsInStream.read(ioBuffer);
				succ = true;
			}
			out.close();
			hdfsInStream.close();
			fs.close();
			System.out.println("write file from " + hdfsSrc + " to " + localDst + " succeed. ");
		} catch (Exception e) {
			System.out.println("write file from " + hdfsSrc + " to " + localDst + " failed. ");
		}
		return succ;
	}

	public synchronized static byte[] readFromHdfsToMem(String hdfsSrc) throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfsSrc));
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		hdfsInStream.close();
		fs.close();
		System.out.println("write file from " + hdfsSrc + " to " + "Mem succeed. ");

		return ioBuffer;
	}

	public synchronized static byte[] readFromHdfs(String hdfsSrc) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		byte[] buffer = null;
		FSDataInputStream hdfsInStream = null;
		try {
			fs = FileSystem.get(URI.create(iNode), conf);
			Path path = new Path(hdfsSrc);
			hdfsInStream = fs.open(path);
			FileStatus stat = fs.getFileStatus(path);
			buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			hdfsInStream.readFully(0, buffer);
			//int readLen = hdfsInStream.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			hdfsInStream.close();
			fs.close();
		}
		//System.out.println("write file from " + hdfsSrc + " to " + "Mem succeed. ");
		return buffer;
	}

	/**
	 * 9 下载目录或文件
	 * 
	 * @throws IOException
	 */
	public synchronized static boolean downloadFromHdfs(String hdfsSrc, String localDst) throws IOException {
		Path srcpath = new Path(hdfsSrc);
		File file = new File(localDst);
		String localpath = file.getPath();
		boolean isok = false;
		try {
			FileSystem fs = FileSystem.get(URI.create(iNode), conf);
			// FileSystem fs = srcpath.getFileSystem(conf);
			fs.copyToLocalFile(false, srcpath, new Path(localDst));
			System.out.println("download from " + hdfsSrc + " to  " + localpath + " successed. ");
			isok = true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("22222" + URI.create(hdfsSrc));
			System.out.println("download from " + hdfsSrc + " to  " + localpath + " failed :");
		}
		return isok;
	}

	/**
	 * 10 遍历HDFS上的文件和目录
	 * 
	 * @param fs
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public synchronized static List<String> listFile(String hdfsPath) throws IOException {
		Path dstpath = new Path(hdfsPath); 
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		List<String> nameList = new ArrayList<String>();
		try {
			String subPath = "";
			if(fs.exists(new Path(hdfsPath))){
				FileStatus[] fList = fs.listStatus(dstpath);
				for (FileStatus f : fList) {
					if (null != f) {
						subPath = new StringBuffer().append(f.getPath().getParent()).append("/")
								.append(f.getPath().getName()).toString();
						if (f.isDirectory()) {
							nameList = listFile(subPath);
						} else {
							nameList.add(subPath);
						}
					}
				}
			}else System.out.println("路徑不存在");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("连接异常");
		} finally {
			close(fs);
		}
		return nameList;
	}

	/**
	 * 11 重命名文件 *
	 * 
	 * @param fs
	 * @param path
	 * @throws IOException
	 */
	public synchronized static boolean rename(String oldName, String newName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		String newPath = (new Path(oldName)).getParent().toString() + "/" + newName;
		System.out.print("new Path:" + newPath);
		Path oldPath = new Path(oldName);
		Path newPath1 = new Path(newPath);
		// System.out.print("new Path:" + newName);
		boolean isok = fs.rename(oldPath, newPath1);
		if (isok) {
			System.out.println("rename success");
		} else {
			System.out.println("rename failure");
		}
		fs.close();
		return isok;
	}

	/**
	 * 12 得到文件的最后修改时间 *
	 * 
	 * @param fs
	 * @param path
	 * @throws IOException
	 */
	private synchronized static Date lastModifiedTime(String hdfsPath) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		Path path = new Path(hdfsPath);
		FileStatus fileStatus = fs.getFileStatus(path);
		long modificationTime = fileStatus.getModificationTime();
		Date modTime = new Date(modificationTime);
		return modTime;
	}

	public static long realTimeReadHdfsDir(String temppath) throws IOException {
		// TODO Auto-generated method stub
		long summary = -1;
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(iNode), conf);
			Path path = new Path(temppath);
			summary = hdfs.getContentSummary(path).getLength();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			summary = -1;
		}

		return summary;
	}
	
	public synchronized static boolean copyToPath(String hdfsSrc, String hdfsDst) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(iNode), conf);
		FileUtil fileUtil = new FileUtil();
		Path srcpath = new Path(hdfsSrc);
		Path dstpath = new Path(hdfsDst);
		fileUtil.copy(fs, srcpath, fs, dstpath, false, conf);
//		fs.moveToLocalFile(srcpath, dstpath);
		
		return true;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// File saveFile = new File("D:/Tomcat
		// 6.0/webapps/CCB/uploads/2014_09_11_03_39_43_900.txt");
		// String localSrc = saveFile.getAbsolutePath();
		// System.out.println("localSrc="+localSrc);
		// String hdfsDest = "hdfs://54.0.88.53:8020/user/hdfs/localimport/" +
		// saveFile.getName();
		// System.out.println("hdfsDst="+hdfsDest);
		// HdfsUtil.uploadToHdfs(localSrc, hdfsDest);
		// System.exit(0);

		// TODO Auto-generated method stub
		String[] arg = { "hdfs://54.0.88.53:8020/user/hdfs/test.txt", "D://test.txt" };
		String newName = "tlbs.txt";
		String localDst = "D://qq.txt";
		String localDir = "D://";
		// String hdfsDst = "hdfs://54.0.96.61:8020/user/hive/detailtest.txt";
		String hdfsDst2 = "hdfs://54.0.88.53:8020/user/kang/kangt.txt";
		String hdfsDst3 = "hdfs://HDPHNYW1:8020/test/test01";
		String hdfsDir = "hdfs://HDPHNYW1:8020/test/HN_RTP_RT_8M_re0+NET+network+1.0+0+networklog.txt.1462868981890";
		String hdfsDir1 = "hdfs://54.0.88.53:8020/user/hdfs/kang";// 创建目录
		
		copyToPath(hdfsDir, hdfsDst3);

		// * 1 检查文件是否已经存在
		// boolean checkFile(String hdfsDst)
		//checkFile("/data/flume/log/hs_err_pid14567.log");
		// *2 在HDFS上创建文件
		// createFile(String path)
		// createFile(hdfsDst2);
		// * 3 写本地文件到HDFS的文件上/追加
		// void writeToHdfs(String localSrc,String hdfsDst)
		// boolean flag = writeToHdfs(localDst,hdfsDst);
		// boolean flag = append(localDst, hdfsDst2);

		// *4 上传文件到HDFS
		// void uploadToHdfs(String localSrc,String hdfsDir)
		// boolean flag = uploadToHdfs(localDst, hdfsDir); //报目录不存在 有问题
		// * 5 删除HDFS文件
		// void rmFile(String hdfsDir1)
//		 boolean flag = rmFile("hdfs://HDPHNYW1:8020/test/Flume/HNAPTS01+DB+INFORMIX+11.50+fispsrv+Online.log.1460140967145");
//		 System.out.println(flag);
		// * 6 创建目录和父目录
		// void mkdirs(String hdfsDir)
		// boolean flag =mkdirs(hdfsDir1); //existed false
		// *7 删除目录和子目录
		// void rmdirs(String hdfsDir)
		// boolean flag = rmdirs(hdfsDir1); //恢复机制删除目录 user

		// * 8 读取文件
		// boolean readFromHdfs(String hdfsSrc,String localDst)
		//boolean flag = readFromHdfs(hdfsDst2, localDst);

		// * 9 下载目录或文件
		// void downloadFromHdfs(String hdfsSrc , String localDst)
		// boolean flag = downloadFromHdfs(hdfsDst,localDir);
		// *10 遍历HDFS上的文件和目录
		// void listFile(String hdfsDir)
		// listFile(hdfsDir);

		// *11重命名
		// boolean flag = rename(hdfsDst2, newName);

		// *12得到文件最后修改时间
		// Date flag = lastModifiedTime(hdfsDst);
		// System.out.println(flag);
		//hdfs://HDPHNYW1:8020/test/Flume/HNAPTS01+DB+INFORMIX+11.50+fispsrv+Online.log.1460140967145
		 
	}
}
