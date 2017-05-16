

import java.util.*;
import java.io.*;

/*
 * @author -- rajatgoyal715
 */

public class ScannerTest {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int sum = 0;
        Map<String,Integer> pedras = new HashMap<String, Integer>();
        for(int i =0; i<n;i++){
            String letras = sc.next();
            String[] l = letras.split("");
            Map<String,Integer> p = new HashMap<String, Integer>();
            for(int x=0; x<l.length;x++){
                p.put(l[x],1);
            }
            for (Map.Entry<String, Integer> map : p.entrySet()) {
                if(pedras.get(map.getKey())!=null){
                    int v = pedras.get(map.getKey());
                    pedras.put(map.getKey(),v+1);
                }else{
                    pedras.put(map.getKey(),1);
                }
            }
        }
        for (Map.Entry<String, Integer> map : pedras.entrySet()) {
        	if(map.getValue().equals(n)){
        		sum++;
        	}
        }
        System.out.println(sum);
    }
	
//    public static void main(String[] args) {
//        Scanner sc = new Scanner(System.in);
//        int n = sc.nextInt();
//        for(int i =0; i<n;i++){
//        	int x = 0;
//            int a = sc.nextInt();
//            int b = sc.nextInt();
//            
//            Double qa = Math.sqrt(a);
//            int v_qa = qa.intValue();
//            if(a==Math.pow(v_qa, 2)){
//            	x++;
//            }
//            Double qb = Math.sqrt(b);
//            int v_qb = qb.intValue();
//            if(b == Math.pow(v_qb, 2)){
//            	x++;
//            }
//            System.out.println(x);
//        }
//    }
    
    
    static long fibo(long n) {
        if (n < 2) {
            return n;
        } else {
            return fibo(n - 1) + fibo(n - 2);
        }
    }
}
