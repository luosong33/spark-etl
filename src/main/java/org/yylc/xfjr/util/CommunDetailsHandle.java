package org.yylc.xfjr.util;

public class CommunDetailsHandle {

    public static void regExPhone() {
        String Str = "a1a7a0,";
//        String phoneReg = "(^\\d+)d(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)(\\d)(^\\d+)";
        String phoneReg = "(\\W+)(\\d+)(\\W+)(\\d+)(\\W+)(\\d+)(\\W+)";
//        System.out.println(Str.replaceAll(phoneReg, "$2$4$6")); // Hello, World.

        String Str1 = ",170,0010,6666";
//        String phoneReg1 = "([0-9]{3})(\\W+)([0-9]{4})(\\W+)([0-9]{4})";
        String phoneReg1 = "(^\\d)(\\d+)(\\W+)(\\d+)(\\W+)(\\d+)";
        System.out.println(Str1.replaceAll(phoneReg1, "$2$4$6")); // Hello, World.

    }

    public static void main(String[] args) {
        // 去除单词与 , 和 . 之间的空格
        String Str = "Hello , World .";
        String pattern = "(\\w)(\\s+)([.,])";
        // $0 匹配 `(\w)(\s+)([.,])` 结果为 `o空格,` 和 `d空格.`
        // $1 匹配 `(\w)` 结果为 `o` 和 `d`
        // $2 匹配 `(\s+)` 结果为 `空格` 和 `空格`
        // $3 匹配 `([.,])` 结果为 `,` 和 `.`
//        System.out.println(Str.replaceAll(pattern, "$1$3")); // Hello, World.

        regExPhone();
    }

}
