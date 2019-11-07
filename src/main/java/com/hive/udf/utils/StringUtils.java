package com.red.udf.utils;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
public class StringUtils {

  public static List<String> country=new ArrayList<String>();

  public static final int CHAR_ENGLISH_MATCH=1;
  public static final int CHAR_SPACE_MATCH=2;
  public static final int CHAR_SIGNAL_MATCH=4;
  public static final int CHAR_NUMBER_MATCH=8;
  public static final int CHAR_FUSS_MATCH=16;
  public static final int CHAR_INVISIBLE_MATCH=32;

  public static final String IP_REGEX="([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";

  static {
    country.add("cn");
    country.add("hk");
    country.add("us");
  }

  public static enum DataType{
    INTEGER,
    DOUBLE,
    STRING,
    LONG
  };

  public static String underlineToHump(String para) {
    StringBuilder result = new StringBuilder();
    String a[] = para.split("_");
    for (String s : a) {
      if (result.length() == 0) {
        result.append(s);
      } else {
        result.append(s.substring(0, 1).toUpperCase());
        result.append(s.substring(1));
      }
    }
    return result.toString();
  }

  public static int compare(String a, String b, DataType type){
    switch(type){
      case DOUBLE:
        return Double.valueOf(a).compareTo(Double.valueOf(b));
      case INTEGER:
        return Integer.valueOf(a).compareTo(Integer.valueOf(b));
      case STRING:
        return a.compareTo(b);
      case LONG:
        return  Long.valueOf(a).compareTo(Long.valueOf(b));
      default:
        return -1;
    }
  }




  public static String multipleSplit(final String s, String... vars) {
    if (s == null ||s.trim().length()==0) { return null; }
    String dest=s.trim();
    for(String var :vars){
      if(var==null||var.trim().length()==0){
        break;
      }
      int pos=0;
      String[] parse= var.trim().split("~");
      if(parse.length>2|| parse.length<1){
        return null;
      }
      if(parse.length==2){
        pos=Integer.parseInt(parse[1]);
      }
      String[] arr=dest.split(parse[0]);
      if(arr.length>pos){
        dest=dest.split(parse[0])[pos];
      }else{
        return null;
      }
    }
    return dest;
  }

  public static String recursiveDecode(String code){
    String temp = null;
    String res =code;
    try{
      do{
        temp=res;
        res=URLDecoder.decode(temp, "UTF-8");
      }while(!temp.equals(res));
    }catch(Exception ex){}
    return res;
  }

  public static boolean isNumeric(String str){
    return str.matches("^[0-9]+$");
  }

  public static boolean isDescriable(String str){
    if(str==null||str.trim().length()==0) return false;
    String temp=str.trim();
    for(int i=0;i<temp.length(); i++){
      if(temp.charAt(i)<='z'&& temp.charAt(i)>='a') return true;
      if(temp.charAt(i)<='Z'&& temp.charAt(i)>='A') return true;
      if(temp.charAt(i)<='\u9fa5'&& temp.charAt(i)>='\u4e00') return true;
    }
    return false;
  }


  public static String extractValidString(String str, int type){
    if(str==null||str.trim().length()==0) return null;
    String temp=str.trim();
    StringBuilder sb=new StringBuilder();
    for(int i=0;i<temp.length(); i++){
      if(isValidChar(temp.charAt(i), type)){
        sb.append(temp.charAt(i));
      }
    }
    return sb.toString();
  }

  public static String extractValidString(String str, int type, String replacement){
    if(str==null||str.trim().length()==0) return null;
    String temp=str.trim();
    StringBuilder sb=new StringBuilder();
    for(int i=0;i<temp.length(); i++){
      if(isValidChar(temp.charAt(i), type)){
        sb.append(temp.charAt(i));
      }else{
        sb.append(replacement);
      }
    }
    return sb.toString();
  }


  public static int getRepeatLevel(String str, int type){
    if(str==null||str.trim().length()==0) return 0;
    String temp=str.trim();
    char c=temp.charAt(0);
    int size=0;
    int maxSize=0;
    StringBuilder sb=new StringBuilder();
    for(int i=0;i<temp.length(); i++){
      if(temp.charAt(i)==c){
        size++;
      }else{
        if(size>1){
          if(isValidChar(c, type) && size>maxSize){
            maxSize=size;
          }
          if(type==0 && isChineseChar(c) &&size>maxSize){
            maxSize=size;
          }
        }
        c=temp.charAt(i);
        size=1;
      }
    }

    if(size>1){
      if(isValidChar(c, type) && size>maxSize){
        maxSize=size;
      }
      if(type==0 && isChineseChar(c) &&size>maxSize){
        maxSize=size;
      }
    }
    return maxSize;
  }




  public static boolean isValidChar(char c, int type){

    if((c<='z' && c>='a') || (c<='Z' && c>='A')){
      return (type&CHAR_ENGLISH_MATCH)>0;
    }

    if(c<='9'&& c>='0'){
      return (type&CHAR_NUMBER_MATCH)>0;
    }

    if(c<='龟' && c>='一'){
      return true;
    }

    if(c>=33&& c<=126){
      return (type&CHAR_SIGNAL_MATCH)>0;
    }
    if(c<=' '){
      return (type&CHAR_SPACE_MATCH)>0;
    }
    if(c>=128){
      return (CHAR_FUSS_MATCH & type)>0;
    }
    return (CHAR_INVISIBLE_MATCH & type)>0;

  }


  public static List<String> split(String s, String rep) {
    if (s == null||s.trim().length()==0||"null".equalsIgnoreCase(s.trim())) { return null; }
    String[] arr = s.trim().split(rep);
    List<String> res=new ArrayList<String>();
    for(String str :arr){
      if(str!=null && str.trim().length()>0){
        res.add(str.trim());
      }
    }
    return res;
  }

  public static String getSlice(String s, String sep, int get) {
    if (s == null) { return null; }
    String[] arr= s.toString().split(sep.toString());
    int index= Math.abs(get);
    if(index>arr.length){
      return null;
    }
    if(get<0){
      return arr[arr.length+get];
    }
    return arr[get-1];
  }

  public static String getSlice(String s, String sep, String sep_new, int from, int to) {
    if (s == null) { return null; }
    String[] arr= s.toString().split(sep.toString());

    int f =0, t=0;
    if(from < 0){
      f = arr.length + from;
    }else{
      f = from -1;
    }
    if(to<0){
      t = arr.length + to;
    }else{
      t = to -1;
    }

    if(f>t || f>=arr.length || t<0){
      return null;
    }
    StringBuilder sb = new StringBuilder(arr[f]);
    for(int i=f+1; i<= Math.min(t, arr.length-1); i++){
      sb.append(sep_new + arr[i]);
    }
    return sb.toString();
  }

  public static String getSlice(String s, String sep, int from, int to) {
    return getSlice(s, sep, sep, from, to);
  }

  public static String getSlice(List<String> str, int index) {
    if(str == null){
      return null;
    }
    int idx= index<0? (str.size()+index):(index-1);
    if( idx>=str.size() ){
      return null;
    }
    return str.get(idx);
  }


  public static String getSlice(List<String> str, String new_sep, int from, int to) {
    if(str == null){
      return null;
    }
    int f= from<0? (str.size()+from):(from-1);
    int t= to<0? (str.size()+to):(to-1);
    if(f>t || f>=str.size() || t<0){
      return null;
    }
    StringBuilder sb = new StringBuilder(str.get(f));
    for(int i=f+1; i<= Math.min(t, str.size()-1); i++){
      sb.append(new_sep + str.get(i));
    }
    return sb.toString();
  }




  public static String extractChineseWord(String s){
    StringBuilder sb =new StringBuilder();
    for(int i=0;i<s.length(); i++){
      if(isChineseChar(s.charAt(i))){
        sb.append(s.charAt(i));
      }
    }
    return sb.toString();
  }

  public static boolean isChineseChar(char c){
    return c>='一'&&c<='龟';
  }


  /**
   * 移除字符串中的乱码
   * @param s
   * @return
   */
  public static String removeEmptyCharacter(String s){
    if(s==null||s.length()==0){
      return null;
    }
    StringBuilder r=new StringBuilder();
    for(int i=0; i<s.length();i++){
      if(s.charAt(i)>=32 && s.charAt(i)!=127){
        r.append(s.charAt(i));
      }
    }
    return r.toString();
  }

  /**
   * 移除字符串中的乱码
   * @param s
   * @return
   */
  public static String replaceInvisibleCharacter(String s, String replacement){
    if(s==null||s.length()==0){
      return null;
    }
    StringBuilder r=new StringBuilder();
    boolean isVisible=true;
    for(int i=0; i<s.length();i++){
      if(s.charAt(i)>32 && s.charAt(i)!=127){
        if(!isVisible){
          r.append(replacement);
        }
        r.append(s.charAt(i));
        isVisible=true;
      }else{
        isVisible=false;
      }
    }
    return r.toString();
  }


  public static String extractNormalCharacter(String s, String replacement){
    return extractNormalCharacter(s,replacement, false);
  }


  public static String extractNormalCharacter(String s, String replacement, boolean includeFuss){
    if(s==null||s.length()==0){
      return null;
    }
    StringBuilder r=new StringBuilder();
    boolean isVisible=true;
    if(includeFuss){
      for(int i=0; i<s.length();i++){
        if((s.charAt(i)>=32 && s.charAt(i)<127)|| (s.charAt(i)>=128)||s.charAt(i)==5){
          if(!isVisible){
            r.append(replacement);
          }
          r.append(s.charAt(i));
          isVisible=true;
        }else{
          isVisible=false;
        }
      }
    }else{
      for(int i=0; i<s.length();i++){
        if((s.charAt(i)>32 && s.charAt(i)<127)|| (s.charAt(i)>='一' && s.charAt(i)<='龟')){
          if(!isVisible){
            r.append(replacement);
          }
          r.append(s.charAt(i));
          isVisible=true;
        }else{
          isVisible=false;
        }
      }
    }

    return r.toString();
  }




  public static String replaceInVisibleChar(String s, String replacement){
    if(s==null||s.length()==0){
      return null;
    }
    StringBuilder r=new StringBuilder();
    boolean isVisible=true;
    for(int i=0; i<s.length();i++){
      if((s.charAt(i)>32 && s.charAt(i)<127)|| (s.charAt(i)>='一' && s.charAt(i)<='龟')){
        if(!isVisible){
          r.append(replacement);
        }
        r.append(s.charAt(i));
        isVisible=true;
      }else{
        isVisible=false;
      }
    }
    return r.toString();
  }


  public static boolean isVisible(char c){
    if(c>=32 &&c<=126){
      return true;
    }
    if(c>=128){
      return true;
    }
    return false;
  }




  public static void main(String[] argc)throws Exception{

    System.out.println( getRepeatLevel("我我我试试你的AAA11111111",0));

    System.out.println((int)('一'));
    System.out.println(  recursiveDecode("%E7%AC%94%E8%AE%B0%E6%9C%AC"));
    System.out.println(  recursiveDecode("%25E5%25A4%259A%25E8%258A%25AC"));
    System.out.println(  recursiveDecode("多芬"));
    System.out.println(isNumeric("00013223"));
    System.out.println(isNumeric("0001322a"));

    System.out.println(isDescriable("00013223"));
    System.out.println(isDescriable("0001322a"));
    System.out.println(isDescriable("  ^ +-*、 &"));
    System.out.println(isDescriable("^中——"));
    System.out.println("http://www.baidu.com/s?ie=utf-8&bs=test&f=8&rsv_bp=1&rsv_spt=3&wd=%E4%B8%AD%E5%8D%8E&rsv_sug3=9&rsv_sug=0&rsv_sug1=8&rsv_sug4=210&inputT=3506");

    System.out.println(replaceInvisibleCharacter("search_keyword	    tp  ", " "));


  }
}
