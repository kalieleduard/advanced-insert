package br.com.example.advancedinsert.controller;

import java.util.ArrayList;
import java.util.List;

public interface ListUtils {

    static <E> List<List<E>> partitionList(final List<E> currentList, final int partitionNumber) {
        final List<List<E>> result = new ArrayList<>();

        int totalSize = currentList.size();
        int baseSize = totalSize / partitionNumber;
        int remainder = totalSize % partitionNumber;

        int start = 0;
        for (int i = 0; i < partitionNumber; i++) {
            int chunkSize = baseSize + (remainder > 0 ? 1 : 0);
            remainder = Math.max(0, remainder - 1);

            int end = start + chunkSize;
            end = Math.min(end, totalSize);

            if (start >= end) {
                break;
            }

            result.add(currentList.subList(start, end));
            start = end;
        }

        return result;
    }
}
