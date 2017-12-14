package entity;

import spark.Person;

class UserPay {
    void pay(double money) {

    }
}

public class UserDTO extends UserPay {

    private Person person;

    public static void main(String[] args) {
        Person person = Person.apply("Andy", 32);
        System.out.println(person.name());
    }

    public void pay(int money) {

    }

    @Override
    public void pay(double money) {
    }

    public boolean pay(double moeny, int type) {
        return false;
    }

}
