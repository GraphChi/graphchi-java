package edu.cmu.graphchi.datablocks;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * GraphChi stores vertex adn edge values in compact byte format.
 * To convert the bytes to Java objects, one needs to define converter
 * objects. Note that the size of the values as bytes is fixed.
 * For an example,
 * @see edu.cmu.graphchi.datablocks.FloatConverter
 * @see edu.cmu.graphchi.datablocks.FloatPairConverter
 * @param <T>
 */
public interface BytesToValueConverter<T> {

    public int sizeOf();

    public T getValue(byte[] array);

    public void setValue(byte[] array, T val);
}

