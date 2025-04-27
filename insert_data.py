#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Vibe Coding - MongoDB Data Insertion Module

This module demonstrates how to insert data into MongoDB with positive energy and good vibes.
It showcases MCP integration for enhanced data flow and operator patterns for clean code.
"""

import os
import json
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Setup vibrant logging with positive energy
logging.basicConfig(
    level=logging.INFO,
    format="✨ %(asctime)s | %(levelname)s | %(message)s ✨",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("vibe_mongo")

# Load environment variables with good vibes
load_dotenv()
logger.info("Positive environment variables loaded successfully")


class VibeMongoOperator:
    """A vibrant MongoDB operator that maintains positive energy throughout database operations"""
    
    def __init__(self, connection_string=None):
        """Initialize with positive connection vibes"""
        # Connect with MCP awareness - use environment variable or default with good energy
        self.connection_string = connection_string or os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.client = None
        self.db = None
        logger.info("MongoDB Vibe Operator initialized with positive energy")
    
    def connect(self, db_name="vibe_db"):
        """Connect to MongoDB with positive vibes"""
        try:
            # Establish connection with good energy
            self.client = MongoClient(self.connection_string)
            self.db = self.client[db_name]
            logger.info(f"Successfully connected to {db_name} with positive energy ✨")
            return True
        except Exception as e:
            # Even errors maintain positive vibes
            logger.error(f"Connection experienced a temporary setback: {str(e)}")
            return False
    
    def insert_one(self, collection_name, data, positive_feedback=True):
        """Insert a single document with positive vibes"""
        try:
            # Add positive timestamp energy to the data
            if isinstance(data, dict):
                data["created_with_positive_vibes_at"] = datetime.now()
                data["energy_level"] = "high"
            
            # Insert with good energy
            result = self.db[collection_name].insert_one(data)
            
            # Spread positive vibes through feedback
            if positive_feedback:
                logger.info(f"Document successfully flowed into {collection_name} with ID: {result.inserted_id} ✨")
            
            return result.inserted_id
        except Exception as e:
            logger.error(f"Insertion faced a challenge: {str(e)}")
            return None
    
    def insert_many(self, collection_name, data_list, maintain_vibe=True):
        """Insert multiple documents while maintaining positive collective energy"""
        try:
            # Enhance each document with positive vibes
            if maintain_vibe and isinstance(data_list, list):
                for data in data_list:
                    data["created_with_positive_vibes_at"] = datetime.now()
                    data["collective_energy"] = "harmonious"
            
            # Bulk insert with good energy flow
            result = self.db[collection_name].insert_many(data_list)
            
            logger.info(f"✨ {len(result.inserted_ids)} documents successfully flowed into {collection_name} ✨")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Bulk insertion encountered a temporary hurdle: {str(e)}")
            return []
    
    def close(self):
        """Close the connection with gratitude"""
        if self.client:
            self.client.close()
            logger.info("Connection closed with gratitude and positive energy")


# MCP Integration Function - demonstrates Model Control Protocol integration
def mcp_enhanced_insert(data_source, collection_name, transformation=None):
    """Insert data with MCP awareness for enhanced vibes"""
    vibe_operator = VibeMongoOperator()
    
    if vibe_operator.connect():
        try:
            # Load data with positive energy
            if isinstance(data_source, str) and os.path.exists(data_source):
                with open(data_source, 'r') as file:
                    if data_source.endswith('.json'):
                        data = json.load(file)
                    else:
                        # Simple CSV-like parsing with good vibes
                        data = []
                        headers = []
                        for i, line in enumerate(file):
                            if i == 0:
                                headers = [h.strip() for h in line.split(',')]
                            else:
                                values = [v.strip() for v in line.split(',')]
                                data.append(dict(zip(headers, values)))
            elif isinstance(data_source, (list, dict)):
                data = data_source
            else:
                logger.warning("Data source format has unique energy we're not attuned to yet")
                return False
            
            # Apply positive transformation if provided
            if transformation and callable(transformation):
                if isinstance(data, list):
                    data = [transformation(item) for item in data]
                else:
                    data = transformation(data)
            
            # Insert with vibes
            if isinstance(data, list):
                result = vibe_operator.insert_many(collection_name, data)
            else:
                result = vibe_operator.insert_one(collection_name, data)
            
            logger.info("MCP-enhanced data flow completed successfully with positive energy")
            vibe_operator.close()
            return result
        except Exception as e:
            logger.error(f"MCP data flow faced a challenge: {str(e)}")
            vibe_operator.close()
            return False
    return False


# Example usage with demonstration data
def main():
    """Demonstrate vibe-based MongoDB insertions"""
    # Sample vibe-infused data
    positive_vibes = [
        {"type": "coding_session", "mood": "energetic", "productivity": "high", "focus_level": 95},
        {"type": "team_collaboration", "mood": "harmonious", "creativity": "flowing", "synergy": "maximum"},
        {"type": "problem_solving", "approach": "positive", "solution_quality": "excellent", "elegance": True}
    ]
    
    # Custom transformation to enhance the vibes
    def enhance_vibes(data):
        data["enhanced"] = True
        data["positive_affirmation"] = "Every line of code creates value and joy"
        return data
    
    # Insert with MCP enhancement
    result = mcp_enhanced_insert(positive_vibes, "vibe_metrics", enhance_vibes)
    
    if result:
        logger.info("✨ Vibe coding demonstration completed successfully! ✨")
        logger.info("✨ Remember to maintain positive energy in all your database operations! ✨")


if __name__ == "__main__":
    main()
