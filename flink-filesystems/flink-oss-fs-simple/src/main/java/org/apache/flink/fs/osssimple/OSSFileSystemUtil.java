package org.apache.flink.fs.osssimple;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.core.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class OSSFileSystemUtil {

    public static final String SEPARATOR = "/";
    public static final char SEPARATOR_CHAR = '/';
    public static final String CUR_DIR = ".";
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");
    private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");
    private static final Pattern SLASHES = Pattern.compile("/+");


    public static Path makeQualified(Path path, URI defaultUri, Path workingDir) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }

        URI pathUri = path.toUri();
        String scheme = pathUri.getScheme();
        String authority = pathUri.getAuthority();
        String fragment = pathUri.getFragment();
        if (scheme == null || authority == null && defaultUri.getAuthority() != null) {
            if (scheme == null) {
                scheme = defaultUri.getScheme();
            }

            if (authority == null) {
                authority = defaultUri.getAuthority();
                if (authority == null) {
                    authority = "";
                }
            }

            URI newUri = null;

            try {
                newUri = new URI(scheme, authority, normalizePath(scheme, pathUri.getPath()), (String)null, fragment);
            } catch (URISyntaxException var10) {
                throw new IllegalArgumentException(var10);
            }

            return new Path(newUri);
        } else {
            return path;
        }
    }

    private static String normalizePath(String scheme, String path) {
        path = SLASHES.matcher(path).replaceAll("/");
        if (WINDOWS && (hasWindowsDrive(path) || scheme == null || scheme.isEmpty() || scheme.equals("file"))) {
            path = StringUtils.replace(path, "\\", "/");
        }

        int minLength = startPositionWithoutWindowsDrive(path) + 1;
        if (path.length() > minLength && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    private static boolean hasWindowsDrive(String path) {
        return WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find();
    }

    private static int startPositionWithoutWindowsDrive(String path) {
        if (hasWindowsDrive(path)) {
            return path.charAt(0) == '/' ? 3 : 2;
        } else {
            return 0;
        }
    }
}
