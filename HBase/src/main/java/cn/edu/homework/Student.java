package cn.edu.homework;

/**
 * @ClassName Student
 * @Author MerLiang
 * @Date 2021_05_07_22:22
 */
public class Student {
    private int id;

    private String username;

    private String password;

    public Student() {
    }

    public Student(int id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
