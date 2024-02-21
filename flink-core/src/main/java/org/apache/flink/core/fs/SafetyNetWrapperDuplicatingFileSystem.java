package org.apache.flink.core.fs;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

public class SafetyNetWrapperDuplicatingFileSystem extends SafetyNetWrapperFileSystem implements DuplicatingFileSystem {

    private final DuplicatingFileSystem duplicatingFileSystem;

    public SafetyNetWrapperDuplicatingFileSystem(
            FileSystem unsafeFileSystem,
            SafetyNetCloseableRegistry registry) {
        super(unsafeFileSystem, registry);
        Preconditions.checkArgument(unsafeFileSystem instanceof DuplicatingFileSystem,
                "The unsafeFileSystem need be DuplicatingFileSystem.");
        this.duplicatingFileSystem = (DuplicatingFileSystem) unsafeFileSystem;
    }

    @Override
    public boolean canFastDuplicate(Path source, Path destination) throws IOException {
        return duplicatingFileSystem.canFastDuplicate(source, destination);
    }

    @Override
    public void duplicate(List<CopyRequest> requests) throws IOException {
        duplicatingFileSystem.duplicate(requests);
    }
}
