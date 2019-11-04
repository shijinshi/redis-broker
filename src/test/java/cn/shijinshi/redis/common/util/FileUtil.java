package cn.shijinshi.redis.common.util;

import java.io.File;

/**
 * @author Gui Jiahai
 */
public class FileUtil {

    public static void delete(File file) {
        if (!file.exists()) {
            return;
        }

        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    delete(f);
                } else {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
        }
        //noinspection ResultOfMethodCallIgnored
        file.delete();
    }



}
