package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.View;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.IntegerType;

import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class ConnectingComponent3D {


    public static <T extends IntegerType<T>, InternalView> Img<T> run(Img<T> img, int threshold, int min_volume) {

        final int imageW = (int) img.dimension(0);
        final int imageH = (int) img.dimension(1);
        final int imageD = (int) img.dimension(2);

        final int w = imageW+2;
        final int h = imageH+2;
        final int d = imageD+2;

        Img<T> newimg = img.factory().create(img.dimension(0), img.dimension(1), img.dimension(2));
        RandomAccess<T> istack = img.randomAccess();
        RandomAccess<T> ostack = newimg.randomAccess();

        int imagesize = w*h*d;

        long start, end;
        start = System.nanoTime();

        //final Pointer<Integer> tptr = Pointer.allocateInts(imagesize);
        Img<T> tmpimg = img.factory().create(w, h, d);
        RandomAccess<T> tstack = tmpimg.randomAccess();

        int minx = imageW-1;
        int miny = imageH-1;
        int minz = imageD-1;
        int maxx = 0;
        int maxy = 0;
        int maxz = 0;
        Cursor<T> minmaxCursor = Views.iterable(img).cursor();
        while (minmaxCursor.hasNext()) {
            minmaxCursor.fwd();
            int val = minmaxCursor.get().getInteger();
            if (val >= threshold) {
                int x = minmaxCursor.getIntPosition(0);
                int y = minmaxCursor.getIntPosition(1);
                int z = minmaxCursor.getIntPosition(2);
                if (minx > x)
                    minx = x;
                if (miny > y)
                    miny = y;
                if (minz > z)
                    minz = z;
                if (maxx < x)
                    maxx = x;
                if (maxy < y)
                    maxy = y;
                if (maxz < z)
                    maxz = z;
            }
        }

        minx -= 2;
        miny -= 2;
        minz -= 2;
        maxx += 2;
        maxy += 2;
        maxz += 2;
        if (minx < 0) minx = 0;
        if (miny < 0) miny = 0;
        if (minz < 0) minz = 0;
        if (maxx > imageW-1) maxx = imageW-1;
        if (maxy > imageH-1) maxy = imageH-1;
        if (maxz > imageD-1) maxz = imageD-1;

        final int boxw = (maxx - minx + 1 > 0) ? maxx - minx + 1 : 0;
        final int boxh = (maxy - miny + 1 > 0) ? maxy - miny + 1 : 0;
        final int boxd = (maxz - minz + 1 > 0) ? maxz - minz + 1 : 0;
        final int fminx = minx;
        final int fminy = miny;
        final int fminz = minz;

        long[] min = {fminx, fminy, fminz}; // Starting coordinates of the box (inclusive)
        long[] max = {fminx+boxw-1, fminy+boxh-1, fminz+boxd-1}; // Ending coordinates of the box (inclusive)

        long[] min2 = {1, 1, 1}; // Starting coordinates of the box (inclusive)
        long[] max2 = {boxw, boxh, boxd}; // Ending coordinates of the box (inclusive)

        // Create a view representing the box-shaped region
        FinalInterval inInterval = new FinalInterval(min, max);
        IntervalView<T> inRegionView = Views.interval(img, inInterval);
        Cursor<T> inCursor = inRegionView.cursor();
        FinalInterval tmpInterval = new FinalInterval(min2, max2);
        IntervalView<T> tmpRegionView = Views.interval(tmpimg, tmpInterval);
        Cursor<T> tmpCursor = tmpRegionView.cursor();
        while (inCursor.hasNext() && tmpCursor.hasNext()) {
            inCursor.fwd();
            tmpCursor.fwd();
            int pixelValue = inCursor.get().getInteger() >= threshold ? 1 : 0;
            tmpCursor.get().setInteger(pixelValue);
        }

        long[] min3 = {2, 2, 2}; // Starting coordinates of the box (inclusive)
        long[] max3 = {boxw-1, boxh-1, boxd-1}; // Ending coordinates of the box (inclusive)

        //flood fill
        int segid = 1;
        for(int z = 1; z < boxd-1; z++) {
            for(int y = 1; y < boxh-1; y++) {
                for(int x = 1; x < boxw-1; x++) {
                    int val = tstack.setPositionAndGet(x, y, z).getInteger();
                    int segval = ostack.setPositionAndGet(x, y, z).getInteger();
                    if (val > 0 && segval == 0) {
                        Queue<Long> que = new ArrayDeque<Long>();
                        int count = 0;
                        long cx, cy, cz;

                        long tmp;

                        ostack.setPositionAndGet(x, y, z).setInteger(segid);

                        que.add(((long)x << 16 | (long)y) << 16 | (long)z);

                        while(que.peek() != null){
                            count++;
                            tmp = que.poll();

                            cx = tmp >> 32;
                            cy = (tmp >> 16) & (long)0xFFFF;
                            cz = tmp & (long)0xFFFF;
                            for(int dz = -1; dz <= 1; dz++){
                                for(int dy = -1; dy <= 1; dy++){
                                    for(int dx = -1; dx <= 1; dx++){
                                        int neighbor_val = tstack.setPositionAndGet(cx + dx, cy + dy, cz + dz).getInteger();
                                        T neighbor_out = ostack.setPositionAndGet(cx + dx, cy + dy, cz + dz);
                                        if(neighbor_val > 0 && neighbor_out.getInteger() == 0){
                                            neighbor_out.setInteger(segid);
                                            que.add(((long)(cx + dx) << 16 | (long)(cy + dy)) << 16 | (long)(cz + dz));
                                        }
                                    }
                                }
                            }
                        }
                        segid++;
                    }
                }
            }
        }

        final Map<Integer, Integer> map = new HashMap<>();
        for(int z = 1; z < boxd-1; z++) {
            for(int y = 1; y < boxh-1; y++) {
                for(int x = 1; x < boxw-1; x++) {
                    int id = z*h*w + y*w + x;
                    int val = ostack.setPositionAndGet(x,y,z).getInteger();
                    if (val != 0) {
                        tstack.setPositionAndGet(x,y,z).setInteger(val);
                        if (map.containsKey(val)) {
                            int num = map.get(val);
                            map.put(val, num+1);
                        } else
                            map.put(val, 1);
                    }
                }
            }
        }

        final int vt = min_volume;

        for (int z = 0; z < boxd; z++) {
            for(int y = 0; y < boxh; y++) {
                for(int x = 0; x < boxw; x++) {
                    int val = tstack.setPositionAndGet(x+1, y+1, z+1).getInteger();
                    if (map.containsKey(val)) {
                        int num = map.get(val);
                        ostack.setPositionAndGet(x+fminx,y+fminy,z+fminz).setInteger((num == -1) ? 0 : num);
                    }
                }
            }
        }

        return newimg;

    }
}
