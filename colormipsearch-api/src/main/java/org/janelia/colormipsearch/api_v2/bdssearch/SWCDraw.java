package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SWCDraw {

    static class Vec4 {
        public double x;
        public double y;
        public double z;
        public double w;
        Vec4(double x_, double y_, double z_, double w_) {
            x = x_; y = y_; z = z_; w = w_;
        }
    }

    static class iVec2 {
        public int x;
        public int y;
        iVec2(int x_, int y_) {
            x = x_; y = y_;
        }
    }

    private static < T extends RealType< T >> void draw3dSphere(RandomAccess<T> img, int x, int y, int z, double r, int w, int h, int d, double zratio)
    {

        int ir = (int)Math.ceil(r);
        int izr = (int)Math.ceil(r/zratio);
        for (int dz = -izr; dz <= izr; dz++) {
            for (int dy = -ir; dy <= ir; dy++) {
                for (int dx = -ir; dx <= ir; dx++) {
                    int xx = x + dx;
                    int yy = y + dy;
                    int zz = z + dz;
                    double dd = dx*dx + dy*dy + dz*zratio*dz*zratio;
                    if (dd <= r*r) {
                        if (xx >= 0 && xx < w && yy >= 0 && yy < h && zz >= 0 && zz < d) img.setPositionAndGet(xx, yy, zz).setReal(255.0f);
                    }
                }
            }
        }
    }

    private static < T extends RealType< T >> void draw3dLine(RandomAccess<T> img, int w, int h, int d, double x1, double y1, double z1, double r1, double x2, double y2, double z2, double r2, double zratio)
    {
        double a = Math.abs(x2 - x1), b = Math.abs(y2 - y1), c = Math.abs(z2 - z1);
        int pattern = 0;
        if(a >= b && a >= c) pattern = 0;
        else if(b >= a && b >= c) pattern = 1;
        else if(c >= a && c >= b) pattern = 2;

        if (r1 <= 0.0) r1 = 0.0;
        if (r2 <= 0.0) r2 = 0.0;
        int ir1 = r1 > 0.0 ? (int)Math.ceil(r1) : 0;
        int ir2 = r2 > 0.0 ? (int)Math.ceil(r2) : 0;

        double stx = x1, sty = y1, stz = z1;
        double edx = x2, edy = y2, edz = z2;
        double dx, dy, dz, dr;
        int ix, iy, iz;
        int iteration;
        int sign;
        double dtmp;

        switch (pattern) {
            case 0:
                sign = x2 > x1 ? 1 : -1;
                dx = sign;
                dy = (y2 - y1) / (x2 - x1);
                dz = (z2 - z1) / (x2 - x1);
                dr = (r2 - r1) / (x2 - x1);
                stx = (int)x1;
                sty = (stx-x1)*dy + y1;
                stz = (stx-x1)*dz + z1;
                edx = (int)x2;
                edy = (edx-x2)*dy + y1;
                edz = (edx-x2)*dz + z1;
                iteration = (int)Math.abs(edx - stx);
                for (int i = 0; i <= iteration; i++) {
                    ix = (int)(stx + i*sign);
                    iy = (int)(sty + dy*i*sign);
                    iz = (int)(stz + dz*i*sign);
                    if (ir1 == 0 && ir2 == 0) {
                        if (ix >= 0 && ix < w && iy >= 0 && iy < h && iz >= 0 && iz < d) img.setPositionAndGet(ix, iy, iz).setReal(255.0f);
                    } else {
                        draw3dSphere(img, ix, iy, iz, r1+dr*i*sign, w, h, d, zratio);
                    }
                }
                break;
            case 1:
                sign = y2 > y1 ? 1 : -1;
                dy = sign;
                dz = (z2 - z1) / (y2 - y1);
                dx = (x2 - x1) / (y2 - y1);
                dr = (r2 - r1) / (y2 - y1);
                sty = (int)y1;
                stz = (sty-y1)*dz + z1;
                stx = (sty-y1)*dx + x1;
                edy = (int)y2;
                edz = (edy-y2)*dz + z1;
                edx = (edy-y2)*dx + x1;
                iteration = (int)Math.abs(edy - sty);
                for (int i = 0; i <= iteration; i++) {
                    iy = (int)(sty + i*sign);
                    iz = (int)(stz + dz*i*sign);
                    ix = (int)(stx + dx*i*sign);
                    if (ir1 == 0 && ir2 == 0) {
                        if (ix >= 0 && ix < w && iy >= 0 && iy < h && iz >= 0 && iz < d) img.setPositionAndGet(ix, iy, iz).setReal(255.0f);
                    } else {
                        draw3dSphere(img, ix, iy, iz, r1+dr*i*sign, w, h, d, zratio);
                    }
                }
                break;
            case 2:
                sign = z2 > z1 ? 1 : -1;
                dz = sign;
                dx = (x2 - x1) / (z2 - z1);
                dy = (y2 - y1) / (z2 - z1);
                dr = (r2 - r1) / (z2 - z1);
                stz = (int)z1;
                stx = (stz-z1)*dx + x1;
                sty = (stz-z1)*dy + y1;
                edz = (int)z2;
                edx = (edz-z2)*dx + x1;
                edy = (edz-z2)*dy + y1;
                iteration = (int)(Math.abs(edz - stz));
                for (int i = 0; i <= iteration; i++) {
                    iz = (int)(stz + i*sign);
                    ix = (int)(stx + dx*i*sign);
                    iy = (int)(sty + dy*i*sign);
                    if (ir1 == 0 && ir2 == 0) {
                        if (ix >= 0 && ix < w && iy >= 0 && iy < h && iz >= 0 && iz < d) img.setPositionAndGet(ix, iy, iz).setReal(255.0f);
                    } else {
                        draw3dSphere(img, ix, iy, iz, r1+dr*i*sign, w, h, d, zratio);
                    }
                }
                break;
        }
    }

    public static Img<?> draw(String swc_path, int w, int h, int d, double vw, double vh, double vd, int r, boolean ignore) {

        double pxsize = vw;
        double zratio = vd / pxsize;

        File fileEntry = new File(swc_path);
        if (!fileEntry.exists())
            return null;

        Map<Integer, Integer> id_corresp = new HashMap<Integer, Integer>();
        ArrayList<Vec4> verts = new ArrayList<Vec4>();
        ArrayList<iVec2> edges = new ArrayList<iVec2>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileEntry));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {

                if (line.isEmpty() || line.charAt(0) == '#')
                {
                    line = br.readLine();
                    continue;
                }

                String[] tokenstemp = line.split(" ", 0);
                ArrayList<String> tokens = new ArrayList<String>();
                for (int i = 0; i < tokenstemp.length; i++) {
                    if (!tokenstemp[i].isEmpty()) tokens.add(tokenstemp[i]);
                }
                if (tokens.size() >= 7)
                {
                    double[] v4 = new double[4];
                    int ival;
                    int id = Integer.parseInt(tokens.get(0));
                    v4[0] = Double.parseDouble(tokens.get(2));
                    v4[1] = Double.parseDouble(tokens.get(3));
                    v4[2] = Double.parseDouble(tokens.get(4));
                    if (!tokens.get(5).equals("NA"))
                        v4[3] = Double.parseDouble(tokens.get(5));
                    else
                        v4[3] = 0.0;

                    int newid = verts.size();
                    id_corresp.put(id, newid);
                    verts.add( new Vec4(v4[0], v4[1], v4[2], v4[3]) );

                    ival = Integer.parseInt(tokens.get(6));
                    if (ival != -1)
                        edges.add(new iVec2(id, ival));
                }
                line = br.readLine();
            }

            for (int i = 0; i < edges.size(); i++)
            {
                iVec2 e = edges.get(i);
                e.x = id_corresp.get(e.x);
                e.y = id_corresp.get(e.y);
            }
            br.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>();
        Img<UnsignedShortType> imp = factory.create(w, h, d);

        for (int i = 0; i < edges.size(); i++) {
            iVec2 e = edges.get(i);
            Vec4 v1 = verts.get(e.x);
            Vec4 v2 = verts.get(e.y);
            double x1 = v1.x / pxsize;
            double y1 = v1.y / pxsize;
            double z1 = v1.z / vd;
            double r1 = (!ignore && v1.w > 0.0) ? v1.w/pxsize*(r>0?r:1) : r ;
            double x2 = v2.x / pxsize;
            double y2 = v2.y / pxsize;
            double z2 = v2.z / vd;
            double r2 = (!ignore && v2.w > 0.0) ? v2.w/pxsize*(r>0?r:1) : r ;
            draw3dLine(imp.randomAccess(), w, h, d, x1, y1, z1, r1, x2, y2, z2, r2, zratio);
        }

        return imp;
    }
}
