extern crate reqwest;
extern crate serde_derive;
extern crate serde_json;

use serde_json::Value;
use std::collections::HashMap;

extern crate amqp;
extern crate env_logger;

use amqp::{Session, Options, Table, Basic, protocol, Channel};
use amqp::AMQPScheme;
use amqp::QueueBuilder;
use amqp::ConsumeBuilder;
use amqp::TableEntry::LongString;
use amqp::protocol::basic;
use std::default::Default;

fn main() {
    println!("entry *****************************");
    let mut props = Table::new();
    props.insert("example-name".to_owned(), LongString("consumer".to_owned()));

    let options = Options {
        host: "127.0.0.1".to_string(),
        port: 5672,
        vhost: "/".to_string(),
        login: "admin".to_string(),
        password: "admin".to_string(),
        frame_max_limit: 131072,
        channel_max_limit: 65535,
        locale: "en_US".to_string(),
        scheme: AMQPScheme::AMQP,
        properties: Table::new(),
    };

    let mut session = Session::new(options).ok().expect("Can't create session");
    let mut channel = session.open_channel(1).ok().expect("Error openning channel 1");
    println!("Openned channel: {:?}", channel.id);

    let queue_name = "test_queue";
    let queue_builder = QueueBuilder::named(queue_name).durable();
    let queue_declare = queue_builder.declare(&mut channel);

    let bind_result = channel.queue_bind("test_queue", "amq.rabbitmq.event", "queue.*", false, Table::new());
    if bind_result.is_ok() {
        println!("queue binding succed!");
    }

//    let mut map = HashMap::new();
    let closure_consumer = move |_chan: &mut Channel, deliver: basic::Deliver, headers: basic::BasicProperties, data: Vec<u8>|
        {
            if deliver.routing_key == "consumer.created" {
                let header = headers.to_owned().headers.unwrap();
//                println!("{:?}", header);
                let queue = header.get("queue").unwrap();
                queue.length();
                println!("{}", queue);
                println!("consumer.created");
            }

            if deliver.routing_key == "consumer.deleted" {
                let header = headers.to_owned().headers.unwrap();
                println!("consumer.deleted");
            }
        };
    let consumer_name = channel.basic_consume(closure_consumer, queue_name, "", false, false, false, false, Table::new());
    println!("Starting consumer {:?}", consumer_name);

    channel.start_consuming();

    channel.close(200, "Bye").unwrap();
    session.close(200, "Good Bye");

    println!("exit *****************************");

}







fn query_cosumers() -> HashMap<String, String> {
    let mut map = HashMap::new();

    let client = reqwest::Client::new();
    let mut resp = client.get("http://localhost:15672/api/consumers")
        .basic_auth("admin", Some("admin"))
        .send().unwrap();

    if resp.status().is_success() {
        let data: Value = serde_json::from_str(resp.text().unwrap().as_str()).unwrap();
        let obj_array = data.as_array().unwrap();

        for obj in obj_array {
            let queue = obj.get("queue").unwrap().as_object().unwrap();
            let name = queue.get("name").unwrap().as_str().unwrap();
            let str_name: String = String::from(name);
            let last_6_code = str_name.clone().get(61..67).unwrap().to_owned();
            map.insert(last_6_code, str_name);
        }
    }

    if !map.is_empty() {
        for (key, val) in map.iter() {
            println!("{}: {}", key, val);
        }
    }

    map
}
