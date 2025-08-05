#!/usr/bin/env python3
"""
Mock data generator for testing the data lake without Docker container
"""

import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any


class MockDataGenerator:
    def __init__(self):
        self.manufacturers = ["Toyota", "Honda", "Ford", "Chevrolet", "BMW", "Mercedes", "Audi", "Jeep ", "Nissan", "Hyundai"]  # Note: "Jeep " has trailing space
        self.models = ["Sedan", "SUV", "Truck", "Coupe", "Hatchback", "Compass", "Camry", "Accord", "F-150"]
        self.door_states = ["LOCKED", "UNLOCKED"]
        self.gear_positions = ["1", "2", "3", "4", "5", "R", "P", "N", "D", "DRIVE", "REVERSE", "PARK"]
        
    def generate_vin(self) -> str:
        """Generate a realistic-looking VIN"""
        chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
        return ''.join(random.choices(chars, k=17))
    
    def generate_vehicle_message(self, timestamp: int = None) -> Dict[str, Any]:
        """Generate a single vehicle message"""
        if timestamp is None:
            # Generate timestamp within last 24 hours
            now = datetime.now()
            random_time = now - timedelta(hours=random.randint(0, 24))
            timestamp = int(random_time.timestamp() * 1000)
        
        # Occasionally include some problematic data for testing
        vin = self.generate_vin()
        if random.random() < 0.01:  # 1% chance of SQL injection attempt
            vin = f"{vin}'; DROP TABLE vehicles; --"
        
        message = {
            "vin": vin if random.random() > 0.005 else None,  # 0.5% null VINs
            "manufacturer": random.choice(self.manufacturers),
            "year": random.randint(2010, 2024),
            "model": random.choice(self.models),
            "latitude": round(random.uniform(25.0, 49.0), 2),
            "longitude": round(random.uniform(-125.0, -66.0), 2),
            "timestamp": timestamp,
            "velocity": random.randint(0, 150),
            "frontLeftDoorState": random.choice(self.door_states) if random.random() > 0.1 else None,
            "wipersState": random.choice([True, False]) if random.random() > 0.1 else None,
            "gearPosition": random.choice(self.gear_positions),
            "driverSeatbeltState": random.choice(["LOCKED", "UNLOCKED"])
        }
        
        return message
    
    def generate_messages(self, count: int) -> List[Dict[str, Any]]:
        """Generate multiple vehicle messages"""
        messages = []
        
        # Generate messages across different hours for partitioning
        base_time = datetime.now() - timedelta(hours=6)
        
        for i in range(count):
            # Spread messages across 6 hours
            hour_offset = (i % 6)
            message_time = base_time + timedelta(hours=hour_offset, minutes=random.randint(0, 59))
            timestamp = int(message_time.timestamp() * 1000)
            
            message = self.generate_vehicle_message(timestamp)
            messages.append(message)
        
        return messages
    
    def save_mock_data(self, filename: str = "mock_data.json", count: int = 10000):
        """Save mock data to a JSON file"""
        messages = self.generate_messages(count)
        
        with open(filename, 'w') as f:
            json.dump(messages, f, indent=2)
        
        print(f"Generated {count} mock messages and saved to {filename}")
        return messages


if __name__ == "__main__":
    generator = MockDataGenerator()
    generator.save_mock_data("mock_vehicle_data.json", 10000)