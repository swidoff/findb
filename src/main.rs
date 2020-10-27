fn main() {
    let v = vec![1, 3, 5];
    print_result(v.binary_search(&0));
    print_result(v.binary_search(&1));
    print_result(v.binary_search(&2));
    print_result(v.binary_search(&3));
    print_result(v.binary_search(&4));
    print_result(v.binary_search(&5));
    print_result(v.binary_search(&6));
}

fn print_result(res: Result<usize, usize>) {
    match res {
        Ok(v) => println!("Ok({})", v),
        Err(v) => println!("Err({})", v),
    }
}
