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
    // อ่านค่า brokers จาก .env
    const brokers = this.configService.get<string>('KAFKA_BROKERS')!.split(',');

    // สร้าง clientId แบบสุ่มป้องกันชนกัน
    const clientId = `product-consumer-${randomUUID()}`;

    // สร้าง Kafka instance
    const kafka = new Kafka({
      clientId,
      brokers,
    });

    // สร้าง consumer instance พร้อมกำหนด groupId
    this.consumer = kafka.consumer({
      groupId: 'product-events-group',
    });
  }

  /**
   * เมื่อ module เริ่มทำงาน:
   * - connect consumer
   * - subscribe topic
   * - start consumer loop
   */
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
    
    // begin consuming messages
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        // Kafka ส่ง Buffer → convert → JSON
        const raw = JSON.parse(message.value.toString());

        console.log('\n🟦 [Consumer] Received Event');
        console.log('Topic:', topic);
        console.log('Partition:', partition);
        console.log('Payload:', raw);
      },
    });
  }
}
