import { Injectable, OnModuleInit } from '@nestjs/common';
import { Kafka, Consumer } from 'kafkajs';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'crypto';

@Injectable()
export class KafkaConsumerService implements OnModuleInit {
  // consumer instance เก็บไว้ที่นี่
  private consumer: Consumer;
  private readonly topic = 'product-events';

  constructor(private readonly configService: ConfigService) {
    // อ่าน environment variable KAFKA_BROKERS จาก .env
    // ใช้ Non-null assertion (!) เพราะคาดว่าจะต้องมีค่าใน .env
    const brokers = this.configService.get<string>('KAFKA_BROKERS')!.split(',');
    const groupId = this.configService.get<string>('KAFKA_GROUP_ID')!;

    // สร้าง clientId แบบสุ่ม (เพื่อให้แต่ละ instance มี id ต่างกัน)
    // ใช้ randomUUID() เพื่อกันซ้ำเมื่อมีหลาย instance
    const clientId = `product-consumer-${randomUUID()}`;

    // สร้าง Kafka client ผ่าน kafkajs โดยส่ง clientId และรายชื่อ brokers
    const kafka = new Kafka({
      clientId,
      brokers,
    });

    // สร้าง consumer instance พร้อมกำหนด groupId
    this.consumer = kafka.consumer({
      groupId: groupId,
    });
  }

  // เรียกอัตโนมัติเมื่อ module ถูกเริ่ม
  // connect consumer, สมัคร subscribe topic, start consumer loop
  async onModuleInit() {
    // connect kafka consumer
    await this.consumer.connect();
    console.log('🔥[Kafka] Consumer connected');

    // subscribe topic
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: false, // อ่านเฉพาะ message ใหม่
    });

    console.log(`🔥[Kafka] Subscribed to topic: ${this.topic}`);

    // เริ่มฟัง message
    // eachMessage จะถูกเรียกทุกครั้งเมื่อมี message ใหม่เข้ามา
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        // Kafka ส่ง Buffer → convert → JSON
        const raw = JSON.parse(message.value.toString());

        // แสดงผล message ที่ได้รับ
        console.log('\n🟦 [Consumer] Received Event');
        console.log('Topic:', topic);
        console.log('Partition:', partition);
        console.log('Message:', raw);
      },
    });
  }
}
