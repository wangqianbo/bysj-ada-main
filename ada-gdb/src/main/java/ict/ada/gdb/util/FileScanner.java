package ict.ada.gdb.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Deque;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileScanner {
	private static final Log LOG = LogFactory.getLog(FileScanner.class);

	private FileSystem fileSystem;
	private Deque<FileStatus> deque;

	public FileScanner(String pathURI) {
		URI uri;
		try {
			uri = new URI(pathURI);
		} catch (URISyntaxException e1) {
			throw new RuntimeException(e1);
		}
		Path filePath = new Path(pathURI);
		try {
			fileSystem = FileSystem.get(uri, new Configuration(true));
			if (!fileSystem.exists(filePath)) {
				throw new IllegalArgumentException(pathURI);
			}
			deque = new LinkedList<FileStatus>();
			deque.addFirst(fileSystem.getFileStatus(filePath));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		LOG.info("Successfully init FileScanner in " + pathURI);
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public Configuration getConf() {
		return fileSystem.getConf();
	}

	public void close() {
		if (fileSystem != null) {
			try {
				fileSystem.close();
			} catch (IOException e) {
				LOG.error("Error close FileSystem.", e);
			}
		}
	}

	public boolean hasNext() {
		while (!deque.isEmpty()) {
			FileStatus status = deque.peekFirst();
			if (status.isDir()) {
				deque.removeFirst();
				try {
					FileStatus[] statusArr = fileSystem.listStatus(status.getPath());
					for (int j = 0; j < statusArr.length; j++) {
						FileStatus s = statusArr[statusArr.length - j - 1];
						if (s.isDir())
							deque.addFirst(s);
					}
					// put files in Deque head
					for (int j = 0; j < statusArr.length; j++) {
						FileStatus s = statusArr[statusArr.length - j - 1];
						if (!s.isDir())
							deque.addFirst(s);
					}
				} catch (IOException e) {
					LOG.error("Fail to list FileStatus.", e);
					return false;
				}
			} else {
				return true;
			}
		}
		return false;
	}

	public Path next() {
		return deque.removeFirst().getPath();
	}

	public static void main(String[] args) {
		FileScanner scanner = new FileScanner("hdfs://10.0.99.18:8020/ada/json-data/lxj/hudong_nanhai_dot.txt");
		while (scanner.hasNext()) {
			Path p = scanner.next();
			System.out.println(p);
		}
	}

}
