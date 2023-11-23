def get_sftp():
    print("sftp gogogo!")


def register(name, sex, *args):
    print(f"name: {name}")
    print(f"sex: {sex}")
    print(f"etc: {args}")


def register2(name, sex, *args, **kwargs):
    print(f"name: {name}")
    print(f"sex: {sex}")
    print(f"etc: {args}")
    email = kwargs.get('email')
    phone = kwargs.get("phone")
    blank = kwargs.get("blank")

    if email:
        print(f"email: {email}")
    if phone:
        print(f"phone: {phone}")
    if blank:
        print("Not empty!")
